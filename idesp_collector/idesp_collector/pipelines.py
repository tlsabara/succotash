# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import pandas as pd
import logging

class IdespCollectorPipeline:
    collected_items = []

    def close_spider(self, spider):
        logging.warning('\n\n\n\nSalvando dados.... \n\n\n')
        df = pd.DataFrame(self.collected_items)
        df.to_csv('./arquivo_scrap.csv', index=False)
        logging.warning('\n\n\n\nDados Salvos!!! \n\n\n')

    def process_item(self, item, spider):
        if isinstance(item, dict):
            for i in item.get('school_performance'):
                self.collected_items.append(i)
                logging.warning('\n>>>> ITEM APPENDED <<<<\n')
        else:
            logging.warning('\n>>>> FORMATO INVALIDO <<<<\n')
        return item
