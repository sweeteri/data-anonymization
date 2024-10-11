import pandas as pd

all_quasi_identifiers = ['Координаты',
                         'Дата и время',
                         'Номер карты',
                         'Бренд',
                         'Категория',
                         'Банк',
                         'Платёжная система',
                         'Количество товаров',
                         'Стоимость',
                         'Магазин'
                         ]


def anonymize_dataset(filename, quasi_params):
    df = pd.read_csv(filename)
    df['Номер карты'] = df['Номер карты'].astype('string')
    df['Стоимость'] = df['Стоимость'].astype('string')
    df['Дата и время'] = pd.to_datetime(df['Дата и время'])

    with open('grouped_attributes/grouped_by_categories.txt', 'r', encoding='utf8') as f:
        grouped_categories = {}
        for i in f:
            category, grouped_category, grouped_brand = i.split(':')
            grouped_categories[category] = [grouped_category.strip(), grouped_brand.strip()]
    with open('grouped_attributes/stores_grouped.txt', 'r', encoding='utf8') as f:
        stores_grouped = {}
        for i in f:
            store_name, store_grouped = i.split(':')
            stores_grouped[store_name] = store_grouped.strip()

    def mask_store(i):
        df.at[i, 'Магазин'] = stores_grouped[df.loc[i, 'Магазин']]

    def mask_coordinates(i):
        df.at[i, 'Координаты'] = 'Санкт-Петербург'

    def mask_time(i):
        df.at[i, 'Дата и время (строка)'] = df.loc[i, 'Дата и время'].strftime('%m.%Y')

    def mask_card_number(i):
        mask_card = lambda card_number: f'****************'
        df.at[i, 'Номер карты'] = mask_card(df.loc[i, 'Номер карты'])

    def mask_category(i):
        df.at[i, 'Категория'] = grouped_categories[df.loc[i, 'Категория']][0]

    def mask_brand(i):
        df.at[i, 'Бренд'] = grouped_categories[df.loc[i, 'Категория']][1]

    def mask_price(i):
        price = int(float(df.at[i, 'Стоимость']))
        if 25000 <= price <= 100000:
            df.at[i, 'Стоимость'] = '25000-1000000'
        else:
            df.at[i, 'Стоимость'] = '1000000-3000000'

    def mask_amount_goods(i):
        amount = df.loc[i, 'Количество товаров']
        df.at[i, 'Количество товаров'] = amount if round(amount / 10) * 10 == 0 else round(amount / 10) * 10

    def mask_bank(i):
        bank_mapping = {'Сбербанк': 'BANK_01', 'ВТБ': 'BANK_02', 'Т-Банк': 'BANK_03', 'Альфа-банк': 'BANK_04'}
        df.at[i, 'Банк'] = bank_mapping[df.loc[i, 'Банк']]

    def mask_pay_system(i):
        payment_system_mapping = {'Visa': 'PS_01', 'MasterCard': 'PS_02', 'Мир': 'PS_03'}
        df.at[i, 'Платёжная система'] = payment_system_mapping[df.loc[i, 'Платёжная система']]

    params = {
        'Координаты': mask_coordinates,
        'Дата и время': mask_time,
        'Категория': mask_category,
        'Бренд': mask_brand,
        'Номер карты': mask_card_number,
        'Банк': mask_bank,
        'Платёжная система': mask_pay_system,
        'Количество товаров': mask_amount_goods,
        'Стоимость': mask_price,
        'Магазин': mask_store,
    }
    for ind in df.index:
        for param in quasi_params:
            params[param](ind)

    df.drop(columns=['Дата и время'], inplace=True)
    df.insert(2, 'Дата и время', df.pop('Дата и время (строка)'))
    return df


def evaluate_data_utility(original_df, anonymized_df):
    changed_values = (original_df != anonymized_df).sum().sum()
    total_values = original_df.size
    percentage_changed = (changed_values / total_values) * 100
    return percentage_changed


def calculate_k_anonymity(df):
    df1 = df.copy()
    grouped = df.groupby(all_quasi_identifiers).size().reset_index(name='count')
    grouped_test = df1.groupby(all_quasi_identifiers).size().reset_index(name='count')
    unique_lines = grouped[grouped['count'] == 1]
    total_rows = len(df)

    percentage_of_deleted_data = 0
    df_cleaned = pd.DataFrame()
    i = 0
    while percentage_of_deleted_data < 4.5:
        df1 = df.copy()
        i += 1
        problematic_groups = grouped_test[grouped_test['count'] < i]
        df_merged = df1.merge(problematic_groups, on=all_quasi_identifiers, how='left')
        df_cleaned = df_merged[df_merged['count'].isna()].drop(columns=['count'])
        percentage_of_deleted_data = 100 - len(df_cleaned) / total_rows * 100

    k_anonymity = df_cleaned.groupby(all_quasi_identifiers).size().min()
    bad_k_values = grouped['count'].value_counts().reset_index(name='frequency')
    bad_k_values.columns = ['k_value', 'group_count']
    bad_k_values = bad_k_values[bad_k_values['k_value'] > k_anonymity]
    top_5_bad_k_values = bad_k_values.nsmallest(5, 'k_value')
    top_5_bad_k_values['percentage_of_rows'] = (top_5_bad_k_values['group_count'] * top_5_bad_k_values[
        'k_value'] / total_rows) * 100
    top_5_bad_k_values.drop('group_count', axis=1, inplace=True)
    return df_cleaned, k_anonymity, top_5_bad_k_values, unique_lines, percentage_of_deleted_data
