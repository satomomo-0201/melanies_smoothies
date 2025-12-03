# Custom Smoothie Order Form & Pending Orders

# Import python packages
import streamlit as st
import requests  # <-- これを追加
from snowflake.snowpark.functions import col
from snowflake.snowpark.functions import when_matched

# Snowparkセッションの取得
cnx = st.connection("snowflake")
session = cnx.session()


# --- 注文フォーム（Custom Smoothie Order Form）のロジック ---

# アプリのタイトル
st.title(":cup_with_straw: Customize Your Smoothie! :cup_with_straw:")
st.write(
  """Choose the fruits you want in your custom Smoothie!
  """)

name_on_order = st.text_input('Name on Smoothie:')
st.write('The name on your Smoothie will be:', name_on_order)

# データベースから利用可能なフルーツのオプションを取得
my_dataframe = session.table("smoothies.public.fruit_options").select(col('FRUIT_NAME')).collect()

# ingredients_list (st.multiselectはリストを期待するため、my_dataframeをリストに変換)
ingredients_list = st.multiselect(
'Choose up to 5 ingredients:'
, [row[0] for row in my_dataframe] 
, max_selections=5
)

time_to_insert = st.button('Submit Order')

if ingredients_list and time_to_insert:
    # 選択されたフルーツをカンマ区切りの文字列に変換
    ingredients_string = ' '

  for fruit_chosen in ingredients_list:
      ingredients_string += fruit_chosen + ' '
      smoothiefroot_response = requests.get("https://my.smoothiefroot.com/api/fruit/watermelon")
      sf_df st.dataframe(data=smoothiefroot_response.json(), use_container_width=True)
