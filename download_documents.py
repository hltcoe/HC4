import argparse
import sys
import logging
import json
import gzip
from pathlib import Path
from hashlib import md5

from collections import defaultdict
from contextlib import contextmanager
from multiprocessing import Pool, Lock
from functools import partial
from tqdm import tqdm

import requests
from urllib3.exceptions import ProtocolError
import newspaper
from warcio.archiveiterator import ArchiveIterator


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
    extracted = newspaper.Article(url)
    
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

def hash_doc(e):
    return md5( (e['title'].strip() + e['text'].strip()).encode('utf-8') ).hexdigest()

def process_cc_file(info, out_paths, validate, disable_tqdm, retry=10):
    cc_file, want_idx = info
    saved_docs = defaultdict(list)
    
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
                assert len(found_idx) == len(want_idx), f"Not finding all needed docs in {cc_files}"

            break

        except AssertionError:
            logging.warning(f"Assertion erroer, retrying {ntried+1} times.")
        except KeyboardInterrupt:
            raise
        except Exception as e:
            logging.warning(f"Connection error {e} on {cc_file}, retrying {ntried+1} times.")
        finally: 
            pbar.close()
    
    for lang, docs in saved_docs.items():
        with write_lock(out_paths[lang], 'a') as fw:
            for d in docs:
                fw.write(json.dumps(d, ensure_ascii=False) + '\n')

    logging.info(f'done-cc-file:{cc_file}')

def read_log(path):
    return set(
        line.split('done-cc-file:')[1].split('--')[0].strip()
        for line in open(path) if 'done-cc-file' in line
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
    done_cc_files = set()
    if logpath.exists():
        if args.restart:
            logpath.unlink()
        elif args.resume:
            done_cc_files = read_log(logpath)
        else:
            raise FileExistsError(f"Log file at {logpath} already exists.")

    logging.basicConfig(
        level=logging.DEBUG,
        filename=logpath,
        filemode=('w' if not args.resume else 'a'),
        format='%(asctime)s %(levelname)-8s [%(name)s] %(message)s'
    )
    logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
    mute_other_loggers()

    if args.resume:
        logging.info(f"Resuming -- already processed {len(done_cc_files)} cc files.")
        
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

    logging.info(f'building dictionaries of document to capture')

    # dict(cc_file -> dict(id -> dict(langs) -> hashs'))
    to_capture = defaultdict(lambda : defaultdict(dict)) 
    for lang, id_files in lang_id_file.items():
        for id_file in tqdm(id_files, desc=f'building dict for {lang}'):
            fp = gzip.open(id_file) if id_file.endswith('.gz') else open(id_file)
            for line in tqdm(fp, desc=f'{lang} -- {id_file}', leave=False):
                line = json.loads(line)
                if line['cc_file'] not in done_cc_files:
                    to_capture[ line['cc_file'] ][ line['id'] ][ lang ] = line['md5']

    logging.info(f'Looking for {sum(len(idx) for idx in to_capture.values())} '
                 f'documents in {len(to_capture)} cc_files')

    if len(to_capture) == 0:
        raise ValueError("No documents need to be captured.")

    worker_ = partial(process_cc_file, out_paths=out_paths, validate=args.with_validate, disable_tqdm=args.jobs > 1)
    if args.jobs > 1:
        with Pool(args.jobs) as pool:
            list(pool.imap_unordered(
                worker_, 
                tqdm(to_capture.items(), desc="All files")
            ))
    else:
        list(map(worker_, tqdm(to_capture.items(), desc="All files")))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--storage', required=True)
    for lang in LANGUAGES:
        parser.add_argument('--'+lang, nargs='+',
                            help=f'file containing {LANG_NAME[lang]} ids')
    parser.add_argument('--jobs', type=int, default=4)
    parser.add_argument('--restart', action='store_true', default=False)
    parser.add_argument('--resume', action='store_true', default=False)

    parser.add_argument('--with_validate', action='store_true', default=False)

    main(parser.parse_args())


"""
python download_documents.py \
--storage data/cc_storage  \
--fas ../multi-hc4/fas/ids.jsonl.gz \
--zho ../multi-hc4/zho/ids.jsonl.gz \
--rus ../multi-hc4/rus/ids.?.jsonl.gz \
--jobs 20 --resume
"""