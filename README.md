# HC4: HLTCOE CLIR Common-Crawl Collection

This repository contains the scripts for downloading and validating scripts for the documents. 
Document ids, topics, and qrel files are in `resources/hc4/`

Required packages for the scripts are recorded in `requirements.txt`. 

## Topics and Qrels

Topics are stored in `jsonl` format and located in `resources/hc4`. The language(s) the topic is annotated for is recored in the `language_with_qrels` field. We provide the English topic title and description for all topics and human translation for the languages that it has qrels for. We also provide machine translation of them in all three languages for all topics.
Narratives(field `narratives`) are all in English and has one entry for each of the languages that has qrels. 
Each topic also has an English report(field `report`) that is designed to record the prior knowledge the searcher has. 

Qrels are stored in the classic TREC style located in `resources/hc4/{lang}`. 

## Download Documents

To download the documents from Common Crawl, please use the following command to download documents.
If you plan to use HC4 with [`ir_datasets`](https://ir-datasets.com/), please specify `~/.ir_datasets/hc4` as the storage or make a soft link to to the directory you wish to store the documents. The document ids and hashs are stored in `resources/hc4/{lang}/ids*.jsonl.gz`. Russian document ids are separated into 8 files. 

```bash
python download_documents.py --storage ./data/ \
                             --zho ./resources/hc4/zho/ids.jsonl.gz \
                             --fas ./resources/hc4/fas/ids.jsonl.gz \
                             --rus ./resources/hc4/rus/ids.*.jsonl.gz \
                             --jobs 4 \
                             --check_hash 
```

If you wish to only download the documents for one language, just specify the id file for the language
you wish to download. 
We encourage using the flag `--check_hash` to varify the documents downloaded match with the 
documents we intend to use in the collection. 
The full description of the arguments can be found when execute with the `--help` flag.

## Validate

After documents are downloaded, please run the `validate_hc4_documents.py` to verify all documents 
are downloaded for each language. 

```bash
python validate_hc4_documents.py --hc4_file ./data/zho/hc4_docs.jsonl \
                                 --id_file ./resources/zho/ids.jsonl.gz \
                                 --qrels ./resources/zho/*.qrels.txt
```

## Reference

If you use this collection, please kindly cite our dataset paper with the following bibtex entry. 

```bibtex
@inproceedings{hc4,
	author = {Dawn Lawrie and James Mayfield and Douglas W. Oard and Eugene Yang},
	title = {{HC4}: A New Suite of Test Collections for Ad Hoc {CLIR}},
	booktitle = {Proceedings of the 44th European Conference on Information Retrieval (ECIR)},
	year = {2022}
}
```

