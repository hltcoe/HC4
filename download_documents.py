import argparse
import sys
import logging
import json
import gzip
from pathlib import Path


from collections import defaultdict
from contextlib import contextmanager
from multiprocessing import Pool, Lock
from functools import partial
from tqdm.auto import tqdm

import requests
import newspaper
from warcio.archiveiterator import ArchiveIterator

from fix_document_order import hash_doc

# Original written as part of https://github.com/complementizer/wcep-mds-dataset
# File was called `extract_wcep_articles.py`
# Modified for HC4

CHINESE = 'zho'
RUSSIAN = 'rus'
PERSIAN = 'fas'

# When adding a new language, fill in argument dict below as well
LANGUAGES = [CHINESE, RUSSIAN, PERSIAN]
LANG_NAME = {CHINESE: 'Chinese', RUSSIAN : 'Russian', PERSIAN : 'Persian'}

file_lock = Lock()
@contextmanager
def write_lock(fn, mode):
    file_lock.acquire()
    f = open(fn, mode)
    try:
        yield f
    finally:
        f.flush()
        f.close()
        file_lock.release()

def read_warc_gz(cc_file):
    url = "https://commoncrawl.s3.amazonaws.com/" + cc_file
    resp = requests.get(url, stream=True)
    for record in ArchiveIterator(resp.raw, arc2warc=True):
        # if (record.rec_type == 'response' and \
        #     record.http_headers.get_header('Content-Type') == 'text/html'):
        if record.content_type == 'application/http; msgtype=response':
            rid = record.rec_headers.get_header('WARC-Record-ID')\
                  .split('uuid:')[1].split('>')[0]
            yield rid, record

def extract_article(record):
    html = record.content_stream().read()
    url = record.rec_headers.get_header('WARC-Target-URI')
    extracted = newspaper.Article("", fetch_images=False)

    extracted.download(input_html=html)
    extracted.parse()

    # time = None if extracted.publish_date is None else extracted.publish_date.isoformat()
    # short hand for above
    time = extracted.publish_date and extracted.publish_date.isoformat()

    return {
        'time': time,
        'title': extracted.title,
        'text': extracted.text,
        'url': url,
    }


def process_cc_file(info, out_paths, validate, disable_tqdm, retry=10):
    cc_file, want_idx = info
    saved_docs = defaultdict(list)

    success = False
    for ntried in range(retry):
        try:
            pbar = tqdm(disable=disable_tqdm, total=len(want_idx))
            found_idx = set()
            for rid, record in read_warc_gz(cc_file):
                if rid in want_idx:
                    doc = {
                        'id': rid,
                        'cc_file': cc_file,
                        **extract_article(record)
                    }

                    for lang_used in want_idx[rid]:
                        if want_idx[rid][lang_used] != hash_doc(doc):
                            if validate:
                                raise AssertionError(f"md5 hash not matched in {lang_used}")
                            logging.warning(f'record-id: {rid}, warn: md5 hash not matched in {lang_used}')
                        saved_docs[lang_used].append(doc)

                    found_idx.add(rid)
                    pbar.update()

                if len(found_idx) == len(want_idx):
                    logging.info(f"Found all needed docs in {cc_file}, early stopping")
                    break

            if validate:
                assert len(found_idx) == len(want_idx), f"Not finding all needed docs in {cc_file}"

            success = True
            break

        except AssertionError:
            logging.warning(f"Assertion erroer, retrying {ntried+1} times.")
        except KeyboardInterrupt:
            raise
        except Exception as e:
            logging.warning(f"Connection error {e} on {cc_file}, retrying {ntried+1} times.")
        finally:
            pbar.close()

    if success:
        for lang, docs in saved_docs.items():
            with write_lock(out_paths[lang], 'a') as fw:
                for d in docs:
                    fw.write(json.dumps(d, ensure_ascii=False) + '\n')

        logging.info(f'done-cc-file:{cc_file}')


def read_doc_file(path):
    return set(
        json.loads(doc)['id']
        for doc in tqdm(open(path), desc=f'Reading downloaded document file from {path}')
    )

def mute_other_loggers():
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('PIL').setLevel(logging.WARNING)
    logging.getLogger('newspaper').setLevel(logging.WARNING)
    logging.getLogger('chardet.charsetprober').setLevel(logging.WARNING)
    logging.getLogger('chardet.universaldetector').setLevel(logging.WARNING)
    logging.getLogger('jieba').setLevel(logging.CRITICAL)
    logging.getLogger('bs4.dammit').setLevel(logging.ERROR)

def main(args):
    if args.restart and args.resume:
        raise ValueError("Cannot restart and resume at the same time.")

    # arguments for the languages
    lang_id_file = {
        lang: getattr(args, lang)
        for lang in LANGUAGES if getattr(args, lang) is not None
    }

    storage = Path(args.storage)
    storage.mkdir(exist_ok=True, parents=True)

    logpath = storage / 'hc4_log.txt'
    if logpath.exists() and args.restart:
        logpath.unlink()

    logging.basicConfig(
        level=logging.DEBUG,
        filename=logpath,
        filemode=('w' if not args.resume else 'a'),
        format='%(asctime)s %(levelname)-8s [%(name)s] %(message)s'
    )
    logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
    mute_other_loggers()

    out_paths = {}
    for lang in lang_id_file:
        out_paths[lang] = storage / lang / 'hc4_docs.jsonl'
        out_paths[lang].parent.mkdir(exist_ok=True, parents=True)
        if out_paths[lang].exists():
            if args.restart:
                logging.warning(f"{out_paths[lang]} exists, will delete for restart.")
                out_paths[lang].unlink()
            elif not args.resume:
                raise FileExistsError(f"File {out_paths[lang]} already exists.")

    if len(out_paths) == 0:
        raise ValueError("No languages to process.")

    downloaded_doc_ids = defaultdict(dict)
    for lang in lang_id_file.keys():
        if out_paths[lang].exists():
            downloaded_doc_ids[lang] = read_doc_file(out_paths[lang])
            logging.info(f"Resuming -- already downloaded {len(downloaded_doc_ids[lang])} {lang} docs.")


    logging.info(f'building dictionaries of document to capture')

    # Dict[cc_file, Dict[id, Dict[langs, hashs] ] ]
    to_capture = defaultdict(lambda : defaultdict(dict))
    for lang, id_files in lang_id_file.items():
        for id_file in tqdm(id_files, desc=f'building dict for {lang}'):
            fp = gzip.open(id_file) if id_file.endswith('.gz') else open(id_file)
            for line in tqdm(fp, desc=f'{lang} -- {id_file}', leave=False):
                line = json.loads(line)
                if line['id'] not in downloaded_doc_ids[lang]:
                    to_capture[ line['cc_file'] ][ line['id'] ][ lang ] = line['md5']

    logging.info(f'Looking for {sum(len(idx) for idx in to_capture.values())} '
                 f'documents in {len(to_capture)} cc_files')

    if len(to_capture) == 0:
        raise ValueError("No documents need to be captured.")

    worker_ = partial(process_cc_file, out_paths=out_paths, validate=args.check_hash, 
                      disable_tqdm=args.jobs>1, retry=args.retry)
    if args.jobs > 1:
        with Pool(args.jobs) as pool:
            list(pool.imap_unordered(
                worker_,
                tqdm(to_capture.items(), desc="All files")
            ))
    else:
        list(map(worker_, tqdm(to_capture.items(), desc="All files")))

if __name__ == '__main__':
    parser = argparse.ArgumentParser("Document download scripts for HC4 from CC.")
    parser.add_argument('--storage', required=True,
                        help='Directory for storing document jsonl files.')
    for lang in LANGUAGES:
        parser.add_argument('--'+lang, nargs='+',
                            help=f'File containing {LANG_NAME[lang]} ids.')
    parser.add_argument('--jobs', type=int, default=4, help='Number of processes.')
    parser.add_argument('--restart', action='store_true', default=False,
                        help='Restart download from scratch.')
    parser.add_argument('--retry', type=int, default=20, 
                        help='Number of retries per CC file when downloading.')
    parser.add_argument('--resume', action='store_true', default=False,
                        help="Resume download.")

    parser.add_argument('--check_hash', action='store_true', default=False,
                        help="Validate document hashes during download.")

    main(parser.parse_args())

