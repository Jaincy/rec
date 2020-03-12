import jieba
import pandas as pd
import numpy as np
import os
import jieba.analyse

data_path = 'data'


def words(x):
    words = jieba.analyse.extract_tags(x, topK=5, withWeight=False, allowPOS=())

    return words


bookcols = ['tid', 'fk_resource_tid', 'fk_classify_tid',
            'icon', 'name', 'book_concen', 'reading_crowd',
            'disable', 'isbn', 'summary', 'buy_way',
            'buy_address', 'base_read_count', 'play_complete_num',
            'time', 'free_flag', 'seq', 'fk_author_tid',
            'author_name', 'title', 'sub_title', 'view_time_start',
            'view_time_end', 'new_flag',
            'publish_time', 'create_time', 'update_time',
            'del_flag']

book_df = pd.read_csv(os.path.join(data_path, 'ods_t_book/part-m-00000'), sep='\t', names=bookcols)

book_df['summary_words'] = book_df['summary'].apply(lambda x: words(x))
book_df['name_words'] = book_df['name'].apply(lambda x: words(x))
book_df['title_words'] = book_df['title'].apply(lambda x: words(x))
book_df['reading_crowd_words'] = book_df['reading_crowd'].apply(lambda x: words(x))

book_df['summary_words'].head(10)
