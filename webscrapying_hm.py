## imports
import os
import re
import logging
import sqlite3
import requests

import pandas as pd
import numpy  as np

from bs4        import BeautifulSoup
from datetime   import datetime

from sqlalchemy import create_engine

## data gathering
def data_gathering(url, headers):

    #request to URL
    page = requests.get(url, headers = headers)

    # BeautifulSoup object
    soup = BeautifulSoup(page.text, 'html.parser')

    # ============== Product Data ===============
    #website showcase
    products = soup.find('ul', class_= 'products-listing small') #find retorna apenas 1 elemento, pois temos apenas 1 UL, uma vitrine, find_all retorna lista

    #list comprehension to get all products id and products category from the first page of 
    product_list = products.find_all('article', class_ = 'hm-product-item')

    # product id
    product_id = [p.get('data-articlecode') for p in product_list]

    # product_category
    product_category = [p.get('data-category') for p in product_list]

    # product name
    product_list = products.find_all('a', class_ = 'link')
    product_name = [p.get_text() for p in product_list]

    #price
    product_list = soup.find_all('span', class_ = 'price regular')
    product_price = [p.get_text() for p in product_list]

    data = pd.DataFrame([product_id, product_category, product_name, product_price]).T
    data.columns = ['product_id', 'product_category', 'product_name', 'product_price']

    return data

## data gathering by product
def data_gathering_by_product(data, headers):

    # empty  dataframe
    df_compositions = pd.DataFrame()

    # unique columns for all products
    aux = []

    cols = ['Art. No.', 'Composition', 'Fit', 'Product safety', 'Size', 'More sustainable materials']
    df_pattern = pd.DataFrame(columns = cols)

    for i in range(len(data)):
        # API Request
        url = 'https://www2.hm.com/en_us/productpage.' + data.loc[i, 'product_id'] + '.html'

        logger.debug('Product: %s', url)
        
        page = requests.get(url, headers = headers)
        
        # Beautiful Soup object
        soup = BeautifulSoup(page.text, 'html.parser')
        
        # ==================== color name =================================
        product_list = soup.find_all( 'a', class_='filter-option miniature active' ) + soup.find_all( 'a', class_='filter-option miniature' )
        
        # color name
        color_name = [p.get( 'data-color' ) for p in product_list]
        
        # product id
        product_id = [p.get( 'data-articlecode' ) for p in product_list]
        
        df_color = pd.DataFrame( [product_id, color_name] ).T
        df_color.columns = ['product_id', 'color_name']
        
        for j in range(len(df_color)): #go through all colors and collect each composition 
            # API Request
            url = 'https://www2.hm.com/en_us/productpage.' + df_color.loc[j, 'product_id'] + '.html'
        
            logger.debug('Color: %s', url)

            page = requests.get(url, headers = headers)

            # Beautiful Soup object
            soup = BeautifulSoup(page.text, 'html.parser')
            
            # ============ Product Name =============roduct_price = soup.find_all('div', class_ = 'primary-row product-item-price')
            product_name = soup.find_all('h1', class_ = 'primary product-item-headline')
    #       product_name = [ p.get_text() for p in product_name]
            product_name = product_name[0].get_text()
        
            # ============ Product Price =============
            product_price = soup.find_all('div', class_ = 'primary-row product-item-price')
            product_price = re.findall(r'\d+.?\d+', product_price[0].get_text())[0]     
            
    #         df_product_name_price = pd.DataFrame([product_name, product_price]).T
            
            # =================== composition =====================
            product_composition_list = soup.find_all( 'div', class_='pdp-description-list-item' )
            product_composition = [list( filter( None, p.get_text().split( '\n' ) ) ) for p in product_composition_list]

            # create composition dataframe
            df_composition = pd.DataFrame(product_composition).T
            df_composition.columns = df_composition.iloc[0]

            # delete first row
            df_composition = df_composition.iloc[1:].fillna(method = 'ffill')

            # remove pocket lining, shell and lining
            df_composition['Composition'] = df_composition['Composition'].str.replace('Pocket lining: ', '', regex = True)
            df_composition['Composition'] = df_composition['Composition'].str.replace('Pocket: ', '', regex = True)
            df_composition['Composition'] = df_composition['Composition'].str.replace('Shell: ', '', regex = True)
            df_composition['Composition'] = df_composition['Composition'].str.replace('Lining: ', '', regex = True)

            # garantee the same number of columns
            df_composition = pd.concat( [df_pattern, df_composition], axis=0 )

            #rename columns
            df_composition.columns = ['product_id','composition','fit','product_safety', 'size', 'sustainable_materials']

            #create columns product name and product price
            df_composition['product_name'] = product_name
            df_composition['product_price'] = product_price
            
            #keep new columns if it shows up
            aux = aux + df_composition.columns.tolist() #to guarantee we have all columns of composition unique values

            # merge data color + composition
            df_composition = pd.merge( df_composition, df_color, how = 'left', on = 'product_id')

            # all products
            df_compositions = pd.concat([df_compositions, df_composition], axis = 0)
        
    # # Join Showroom data + details
    df_compositions['style_id'] = df_compositions['product_id'].apply( lambda x: x[:-3] )
    df_compositions['color_id'] = df_compositions['product_id'].apply( lambda x: x[-3:] )

    #scrapy time 
    df_compositions['scrapy_datetime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    return df_compositions

## Data Cleaning
def data_cleaning(data_product):

    #product id
    df_data = data_product.dropna(subset = ['product_id'])

    #product name

    df_data['product_name'] = df_data['product_name'].apply(lambda x: x.replace('\n', ''))
    df_data['product_name'] = df_data['product_name'].apply(lambda x: x.replace('\t', ''))
    df_data['product_name'] = df_data['product_name'].apply(lambda x: x.replace('  ', ''))
    df_data['product_name'] = df_data['product_name'].apply(lambda x: x.replace(' ', '_').lower())

    #product price
    df_data['product_price'] = df_data['product_price'].astype(float)

    #color name
    df_data['color_name'] = df_data['color_name'].apply(lambda x: x.replace(' ', '_').replace('/', '_').lower() if pd.notnull(x) else x)

    # #fit
    df_data['fit'] = df_data['fit'].apply(lambda x: x.replace(' ', '_').lower() if pd.notnull(x) else x)

    # #====  size  ======
    # #size number
    df_data['size_number'] = df_data['size'].apply(lambda x: re.search('\d{3}cm', x).group(0) if pd.notnull(x) else x)
    df_data['size_number'] = df_data['size_number'].apply(lambda x: re.search('\d+', x).group(0) if pd.notnull(x) else x) #group(0) locates the whole match expression

    # #size model 
    df_data['size_model'] = df_data['size'].str.extract('(\d+/\\d+)') #.str to vectorize the lines, .extracts cant be applied in the whole column

    # # #product safety

    # =================== sustainable materials ============
    df_data['sustainable_materials'] = df_data['sustainable_materials'].apply(lambda x: x.replace(' ', '_').lower() if pd.notnull(x) else x)

    # df2 = df_data['sustainable_materials'].str.split()

    #recycled cotton / recycled polyester
    # creating an empty dataframe as reference to organize the wnanted columns
    # thenconcatenete with the main dataframe, but it has to have the same lenght as 'data' dataframe

    df2 = df_data[['sustainable_materials']].reset_index(drop=True)
    df_ref2 = pd.DataFrame(index = np.arange(len(df_data)), columns = ['recycled_cotton', 'recycled_polyester'])

    # ------------ recycled cotton ------------
    df_recycled_cotton = df2.loc[df2['sustainable_materials'].str.contains('recycled_cotton', na = True), 'sustainable_materials']
    df_recycled_cotton.name = 'recycled_cotton'

    df_ref2 = pd.concat([df_ref2, df_recycled_cotton], axis = 1)
    df_ref2 = df_ref2.iloc[:, ~df_ref2.columns.duplicated(keep='last')]

    # ------------recycled polyester -------------
    df_recycled_polyester = df2.loc[df2['sustainable_materials'].str.contains('recycled_polyester', na = True), 'sustainable_materials']
    df_recycled_polyester.name = 'recycled_polyester'

    df_ref2 = pd.concat([df_ref2, df_recycled_polyester], axis = 1)
    df_ref2 = df_ref2.iloc[:, ~df_ref2.columns.duplicated(keep='last')]

    # ====================  composition =====================
    #break composition by comma
    df1 = df_data['composition'].str.split(',', expand = True).reset_index(drop=True)

    # cotton / polyester / spandex / 
    # creating empty dataframe as reference to organize the wanted columns and 
    # then concatanete with the main dataframe, but it has to have the same lenght as 'data' dataframe

    df_ref = pd.DataFrame(index = np.arange(len(df_data)), columns = ['cotton', 'polyester', 'spandex'])

    # --------------- cotton ----------------
    df_cotton_0 = df1.loc[df1[0].str.contains('Cotton', na = True), 0]
    df_cotton_0.name = 'cotton'

    df_cotton_1 = df1.loc[df1[1].str.contains('Cotton', na = True), 1]
    df_cotton_1.name = 'cotton'

    #combine
    df_cotton = df_cotton_0.combine_first(df_cotton_1)

    df_ref = pd.concat([df_ref, df_cotton], axis = 1)
    df_ref = df_ref.iloc[:, ~df_ref.columns.duplicated( keep = 'last')] #exclue colunas duplicadas, mantem as diferentes
    # df_ref['cotton'] = df_ref['cotton'].fillna('Cotton 0%')

    # -------------- polyester ----------------
    df_polyester_0 = df1.loc[df1[0].str.contains('Polyester', na = True), 0]
    df_polyester_0.name = 'polyester'

    df_polyester_1 = df1.loc[df1[1].str.contains('Polyester', na = True), 1]
    df_polyester_1.name = 'polyester'

    # combine
    df_polyester = df_polyester_0.combine_first(df_polyester_1)

    # add to reference dataframe
    df_ref = pd.concat([df_ref, df_polyester], axis = 1)
    df_ref = df_ref.iloc[:, ~df_ref.columns.duplicated(keep = 'last')] #delet duplicated columns and keep the different ones

    # -------------- spandex ------------------
    df_spandex_1 = df1.loc[df1[1].str.contains('Spandex', na = True), 1]
    df_spandex_1.name = 'spandex'

    df_spandex_2 = df1.loc[df1[2].str.contains('Spandex', na = True), 2]
    df_spandex_2.name = 'spandex'

    # combine
    df_spandex = df_spandex_1.combine_first(df_spandex_2)

    df_ref = pd.concat([df_ref, df_spandex], axis = 1)
    df_ref = df_ref.iloc[:, ~df_ref.columns.duplicated(keep='last')]

    # # ------------- elasterell -----------
    # df_elasterell = df1.loc[df1[1].str.contains('Elasterell', na = True), 1]
    # df_elasterell.name = 'elasterell'

    # df_ref = pd.concat([df_ref, df_elasterell], axis = 1)
    # df_ref = df_ref.iloc[:, ~df_ref.columns.duplicated(keep='last')]

    # combine the two dataframe reference
    df_ref_final = pd.concat([df_ref, df_ref2], axis = 1)

    # add product_id in the aux dataframe
    df_aux = pd.concat([df_data['product_id'].reset_index(drop = True), df_ref_final], axis = 1)

    # formatt composition data
    df_aux['cotton'] = df_aux['cotton'].apply(lambda x: int(re.search('\d+', x).group(0))/100 if pd.notnull(x) else x)
    df_aux['polyester'] = df_aux['polyester'].apply(lambda x: int(re.search('\d+', x).group(0))/100 if pd.notnull(x) else x)
    df_aux['spandex'] = df_aux['spandex'].apply(lambda x: int(re.search('\d+', x).group(0))/100 if pd.notnull(x) else x)
    df_aux['recycled_cotton'] = df_aux['recycled_cotton'].apply(lambda x: int(re.search('\d+', x).group(0))/100 if pd.notnull(x) else x)
    df_aux['recycled_polyester'] = df_aux['recycled_polyester'].apply(lambda x: int(re.search('\d+', x).group(0))/100 if pd.notnull(x) else x)

    # final join
    df_aux = df_aux.groupby('product_id').max().reset_index().fillna( 0 ) #choose the highest value between lines
    df_data = pd.merge(df_data, df_aux, on = 'product_id', how = 'left')

    #d rop columns
    df_data = df_data.drop(columns = ['size', 'product_safety','composition','sustainable_materials'], axis = 1)

    #drop duplicates
    df_data = df_data.drop_duplicates()

    return df_data

## Data Insert
def data_insert(df_data):

    data_insert = df_data[[
        'product_id',
        'style_id',
        'color_id',
        'product_name',
        'color_name',
        'fit',
        'product_price',
        'size_number',
        'size_model',
        'cotton',
        'polyester',
        'spandex',
        'recycled_cotton',
        'recycled_polyester',
        'scrapy_datetime'
    ]]

    # create database connection
    conn = create_engine( 'sqlite:///database_hm.sqlite', echo = False)

    # data insert
    data_insert.to_sql('vitrine', con = conn, if_exists = 'append', index = False )

    return None

# main function
if __name__ == '__main__':
    # logging
    path = '/home/marxcerqueira/repos/python-ds-ao-dev/'
    
    if not os.path.exists(path + 'Logs'):
        os.makedirs(path + 'Logs')

    logging.basicConfig(
        filename = path + 'Logs/webscraping_hm.log',
        level = logging.DEBUG,
        format = '%(asctime)s - %(levelname)s - %(name)s - %(message)s',
        datefmt = '%Y-%m-%d %H:%M:%S'
        )

    logger = logging.getLogger('webscraping_hm')
    # data parameters and constants
    # parameters
    headers = {'user-agent': 'my-app/0.0.1'}
    
    # URL
    url = 'https://www2.hm.com/en_us/men/products/jeans.html'

    # data gathering
    data = data_gathering(url, headers)
    logger.info('data collect is done')

    # data gathering by product
    data_product = data_gathering_by_product(data, headers)
    logger.info('data gathering by product is done')

    # data cleaning
    data_product_cleaned = data_cleaning(data_product)
    logger.info('data product cleaned is done')

    # data insert
    data_insert(data_product_cleaned)
    logger.info('data insert is done')