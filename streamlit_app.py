# Import python packages
import streamlit as st
import requests 
from snowflake.snowpark.functions import col
import pandas as pd
import json # JSONデータの処理のために追加

# Snowparkセッションの取得
# Streamlitのコンポーネントとして接続名を直接指定
# NOTE: 'snowflake'という接続名はStreamlit環境で事前に設定されている必要があります。
cnx = st.connection("snowflake")
session = cnx.session()


# --- 注文フォーム（Custom Smoothie Order Form）のロジック ---

# アプリのタイトル
st.title(":cup_with_straw: Customize Your Smoothie! :cup_with_straw:")
st.write(
  """Choose the fruits you want in your custom Smoothie!
  """)

# 1. 注文者の名前
name_on_order = st.text_input('Name on Smoothie:')

# 2. データベースからフルーツオプションを取得
# データを取得し、キャッシュを使用して高速化
# 関数がPandas DataFrameを返すように修正
@st.cache_data
def get_fruit_options():
    # FRUIT_NAMEとSEARCH_ONカラムのみを取得し、Pandas DataFrameとして返す
    return session.table("smoothies.public.fruit_options")\
        .select(col('FRUIT_NAME'), col('SEARCH_ON'))\
        .to_pandas()

# my_dataframe は now Pandas DataFrame
pd_df = get_fruit_options()

# 3. 選択肢リストの作成
ingredients_list = st.multiselect(
    'Choose up to 5 ingredients:'
    , pd_df['FRUIT_NAME'].tolist() # リストを渡す
    , max_selections=5
)

# 4. 注文ロジックの定義
# 選択された材料がある場合にのみ処理を実行
if ingredients_list:
    
    # 選択されたフルーツの栄養情報を表示
    st.subheader('Selected Ingredients and Nutrition:')
    
    # 選択されたフルーツのリストを処理
    for fruit_chosen_display in ingredients_list:

        # Pandas DataFrameから対応する 'SEARCH_ON' の値を取得
        # loc[条件, カラム名] でフィルタリングし、最初の値 (.iloc[0]) を取得
        # フィルタリングされた結果が空でないことを前提とする (データ品質による)
        search_on_row = pd_df.loc[pd_df['FRUIT_NAME'] == fruit_chosen_display]
        
        if not search_on_row.empty:
            search_on = search_on_row['SEARCH_ON'].iloc[0]

            # 外部APIから栄養情報を取得
            smoothiefroot_response = requests.get("https://my.smoothiefroot.com/api/fruit/" + search_on)
            
            st.write(f"**{fruit_chosen_display}** ({search_on})")
            
            try:
                # APIレスポンスをJSONとして解析
                nutrition_data = smoothiefroot_response.json()
                
                # APIレスポンスがリストではなく辞書の場合に対応するため、リストにラップしてデータフレーム表示
                st.dataframe(data=pd.DataFrame([nutrition_data]), use_container_width=True)
            except json.JSONDecodeError:
                st.warning(f"Warning: Could not decode JSON for {fruit_chosen_display}. Raw response: {smoothiefroot_response.text[:50]}...")
        else:
            st.warning(f"Error: Could not find 'SEARCH_ON' value for {fruit_chosen_display}.")
            
    # 注文処理のトリガーボタン
    time_to_insert = st.button('Submit Order')
    
    # 5. 注文ボタンが押された場合の処理
    if time_to_insert:
        if name_on_order:
            
            # 注文内容を文字列に変換
            ingredients_string = ', '.join(ingredients_list)
            
            # --- Snowflakeへのデータ挿入処理 ---
            # プレースホルダーを使用してSQLインジェクションのリスクを低減
            insert_query = f"""
                INSERT INTO smoothies.public.orders (ingredients, name_on_order)
                VALUES ('{ingredients_string}', '{name_on_order}')
            """
            
            # データベースへの挿入実行
            try:
                session.sql(insert_query).collect()
                # 成功メッセージの表示
                st.success('Your Smoothie is on its way, ' + name_on_order + '!', icon="✅")
            except Exception as e:
                st.error(f"Failed to submit order to Snowflake: {e}")
            
        else:
            st.warning("Please enter your name before submitting the order.")
            
else:
    st.info("Select your ingredients to see the nutrition information.")
