BOT_NAME = "prajna_sebi"
SPIDER_MODULES = ["prajna_sebi.spiders"]
NEWSPIDER_MODULE = "prajna_sebi.spiders"

ITEM_PIPELINES = {
    "prajna_sebi.pipelines.PrajnaIngestionPipeline": 300,
}

LOG_LEVEL = "INFO"
LOG_FILE = "logs/crawl.log"

REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
FEED_EXPORT_ENCODING = "utf-8"
