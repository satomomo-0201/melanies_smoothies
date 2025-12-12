# Import python packages
import streamlit as st
import requests 
from snowflake.snowpark.functions import col
import pandas as pd
import json 
# import datetime は不要

# Snowparkセッションの取得
cnx = st.connection("snowflake")
session = cnx.session()


# --- 1. データ取得 (キャッシュを使用) ---

@st.cache_data
def get_fruit_options():
    # FRUIT_NAMEとSEARCH_ONカラムのみを取得し、Pandas DataFrameとして返す
    return session.table("smoothies.public.fruit_options")\
        .select(col('FRUIT_NAME'), col('SEARCH_ON'))\
        .to_pandas()

pd_df = get_fruit_options()


# --- 2. フォームの構築 ---

st.title(":cup_with_straw: Customize Your Smoothie! :cup_with_straw:")
st.write("""Choose the fruits you want in your custom Smoothie!""")

name_on_order = st.text_input('Name on Smoothie:')

ingredients_list = st.multiselect(
    'Choose up to 5 ingredients:'
    , pd_df['FRUIT_NAME'].tolist()
    , max_selections=5
)


# --- 3. 栄養素情報の表示と API 呼び出し ---

if ingredients_list:
    
    st.subheader('Selected Ingredients and Nutrition:')
    
    # 選択されたフルーツのリストを処理
    for fruit_chosen_display in ingredients_list:

        search_on_row = pd_df.loc[pd_df['FRUIT_NAME'] == fruit_chosen_display]
        
        if not search_on_row.empty:
            search_on = search_on_row['SEARCH_ON'].iloc[0]

            # 外部APIから栄養情報を取得
            smoothiefroot_response = requests.get(f"https://my.smoothiefroot.com/api/fruit/{search_on}")
            
            st.write(f"**{fruit_chosen_display}** ({search_on})")
            
            try:
                nutrition_data = smoothiefroot_response.json()
                st.dataframe(data=pd.DataFrame([nutrition_data]), use_container_width=True)
            except json.JSONDecodeError:
                st.warning(f"Error: Could not retrieve valid nutrition data for {fruit_chosen_display}.")
        else:
            st.warning(f"Error: Could not find 'SEARCH_ON' value for {fruit_chosen_display}.")
            

# --- 4. 注文の挿入 (ボタンアクション) ---

time_to_insert = st.button('Submit Order')

if time_to_insert:
    if name_on_order and ingredients_list:
        
        # ★★★ 修正箇所: 区切り文字をスペースにし、末尾のスペースを削除（strip） ★★★
        # DORAのハッシュチェックに合わせるため、区切り文字は課題で成功した形式（スペース）を使用
        ingredients_string = ' '.join(ingredients_list).strip()
        
        # Snowflakeへのデータ挿入クエリ (ORDER_TSとORDER_FILLEDを含む)
        insert_query = f"""
          INSERT INTO smoothies.public.orders 
          (ingredients, name_on_order, order_ts, order_filled)
          VALUES (
            '{ingredients_string}', 
            '{name_on_order}', 
            CURRENT_TIMESTAMP(),
            FALSE
          )
        """
        
        try:
            session.sql(insert_query).collect()
            st.success('Your Smoothie is on its way, ' + name_on_order + '!', icon="✅")
        except Exception as e:
            st.error(f"Failed to submit order to Snowflake: {e}")
            
    elif not name_on_order:
        st.warning("Please enter your name before submitting the order.")
    elif not ingredients_list:
        st.warning("Please select at least one ingredient.")
        
else:
    st.info("Select your ingredients to see the nutrition information.")
