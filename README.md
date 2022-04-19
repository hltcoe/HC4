# HC4: HLTCOE CLIR Common-Crawl Collection

This repository contains the scripts for downloading and validating scripts for the documents. 
Document ids, topics, and qrel files are in `resources/hc4/`

Required packages for the scripts are recorded in `requirements.txt`. 

We recommand creating a new python environment for downloading. Package versions could have some unintentional effect on decoding 
the documents from Common Crawl. Documents could have changed on the Common Crawl file for numerous reasons, including take down requests.
When a document changes, we record them in a [change log document](./changed_docs.md). 
Please raise and issue if you have documents with mismatch hashs that are not yet recorded. 

## Topics and Qrels

Topics are stored in `jsonl` format and located in `resources/hc4`. The language(s) the topic is annotated for is recored in the `language_with_qrels` field. We provide the English topic title and description for all topics and human translation for the languages that it has qrels for. We also provide machine translation of them in all three languages for all topics.
Narratives(field `narratives`) are all in English and has one entry for each of the languages that has qrels. 
Each topic also has an English report(field `report`) that is designed to record the prior knowledge the searcher has. 

Qrels are stored in the classic TREC style located in `resources/hc4/{lang}`. 

## Download Documents

To download the documents from Common Crawl, please use the following command.
If you plan to use HC4 with [`ir_datasets`](https://ir-datasets.com/), please specify `~/.ir_datasets/hc4` as the storage or make a soft link to to the directory you wish to store the documents. The document ids and hashs are stored in `resources/hc4/{lang}/ids*.jsonl.gz`. Russian document ids are separated into 8 files. 

```bash
python download_documents.py --storage ./data/ \
                             --zho ./resources/hc4/zho/ids.jsonl.gz \
                             --fas ./resources/hc4/fas/ids.jsonl.gz \
                             --rus ./resources/hc4/rus/ids.*.jsonl.gz \
                             --jobs 4
```

If you wish to only download the documents for one language, just specify the id file for the language
you wish to download. 
In case the URLs for the Common Crawl files change in the future, the flag `--cc_base_url` provides the options 
to specify an alternative URL for the files. The current default value points to `https://data.commoncrawl.org/`. 
The full description of the arguments can be found when execute with the `--help` flag.

## Postprocessing of the Downloaded Documents

Multiprocessing during download results in arbitrary ordering of the documents in the saved `.jsonl` files. 
To support full reproducibility, we provide script to postprocess the file to match the document order specified in the document id files. 
`fix_document_order.py` changes the ordering of the documents, validates the document hashs, and verifies all and only specified documents are in 
the result file. The unsorted file will be renamed as `hc4_docs.jsonl.bak`. You could delete the file manually. Following is a sample command. 

```bash
python fix_document_order.py --hc4_file ./data/rus/hc4_docs.jsonl \
                             --id_file ./resources/hc4/rus/ids*.jsonl.gz \
                             --check_hash
```

**If the script identifies missing files during postprocessing, please rerun the downloading script with `--resume` flag to get the missing documents.**
**Some files might be missing due to temporary network failure or connection refused by the Common Crawl servers.**
**Rerunning the downloading script usually would be able to retrieve those documents. If not, please raise issue with the document id to bring this to our attention.**

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

