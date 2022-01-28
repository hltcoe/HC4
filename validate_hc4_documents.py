import json
import gzip
import argparse

from tqdm.auto import tqdm

def get_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--hc4_file', help='file containing the documents for a particular lang', required=True)
    parser.add_argument('--id_file', help='file containing ids for the language', nargs='+', required=True)
    parser.add_argument('--qrels', help='file containing qrels for the language', metavar='qRel', nargs='+', required=True)
    return parser.parse_args()

def validate_docs(args):
    # Create a set of ids in collection
    doc_ids = set(
        json.loads(doc)['id']
        for doc in tqdm(open(args.hc4_file), desc='Reading downloaded document file')
    )

    # Create a set of relevant docs an non-relevant docs
    rel_docs = set()
    nonrel_docs = set()
    for qrel_fn in args.qrels:
        with open(qrel_fn) as fin:
            for line in fin:
                split = line.strip().split()
                if split[-1] == '0':
                    nonrel_docs.add(split[2])
                else:
                    rel_docs.add(split[2])

    # Report missing documents
    miss_cnt = 0
    rel_miss_cnt = 0
    nonrel_miss_cnt = 0
    for id_file in args.id_file:
        fp = gzip.open(id_file, 'rt') if id_file.endswith('.gz') else open(id_file)
        for doc_info in fp:
            doc_id = json.loads(doc_info)['id']
            if not doc_id in doc_ids:
                miss_cnt += 1
                if doc_id in rel_docs:
                    print(f'{doc_id} MISSING RELEVANT DOCUMENT')
                    rel_miss_cnt += 1
                elif doc_id in nonrel_docs:
                    print(f'{doc_id} missing non-relevant document')
                    nonrel_miss_cnt += 1
                else:
                    print(f'{doc_id} missing document')

    print('MISSING DOCUMENTS:', miss_cnt)
    print('MISSING RELEVANT DOCUMENTS:', rel_miss_cnt)
    print('MISSING NON-RELEVANT DOCUMENTS:', nonrel_miss_cnt)


if __name__ == '__main__':
    validate_docs(get_arguments())
