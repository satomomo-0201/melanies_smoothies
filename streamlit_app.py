# Import python packages
import streamlit as st
import requests 
from snowflake.snowpark.functions import col
from snowflake.snowpark.functions import when_matched
import pandas as pd

# Snowparkセッションの取得
# Streamlitのコンポーネントとして接続名を直接指定
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
@st.cache_data
def get_fruit_options():
    # FRUIT_NAMEとSEARCH_ONカラムのみを取得
    return session.table("smoothies.public.fruit_options").select(col('FRUIT_NAME'), col('SEARCH_ON')).collect()

my_dataframe = get_fruit_options()

# Snowpark DataframeをPandas Dataframeに変換（LOC関数のために必要）
pd_df = my_dataframe.to_pandas()

# 3. 選択肢リストの作成 (Pandas DataFrameからFRUIT_NAMEカラムをリスト化)
ingredients_list = st.multiselect(
    'Choose up to 5 ingredients:'
    , pd_df['FRUIT_NAME'].tolist() # リストを渡す
    , max_selections=5
)

# 4. 注文ロジックの定義
# 選択された材料がある場合にのみ処理を実行
if ingredients_list:
    
    # 選択されたフルーツの栄養情報を表示
    st.subheader('Selected Ingredients:')
    
    # 選択されたフルーツのリストを処理
    for fruit_chosen_display in ingredients_list:

        # 🚨 重要な修正点: fruit_chosen 変数は存在しないため、ループ変数 fruit_chosen_display を使用
        # Pandas DataFrameから対応する 'SEARCH_ON' の値を取得
        search_on = pd_df.loc[pd_df['FRUIT_NAME'] == **fruit_chosen_display**, 'SEARCH_ON'].iloc[0]
        
        # 選択されたフルーツの栄養情報を表示
        st.subheader(fruit_chosen_display + ' Nutrition Information')
        
        # API呼び出しに SEARCH_ON の値を使用
        smoothiefroot_response = requests.get("https://my.smoothiefroot.com/api/fruit/" + search_on)
        
        # APIレスポンスをデータフレームとして表示
        # APIレスポンスがJSONのリストではなく単一の辞書である場合を考慮し、pd.DataFrameでラップ
        st.dataframe(data=pd.DataFrame([smoothiefroot_response.json()]), use_container_width=True)
        
    # 注文処理のトリガーボタン
    time_to_insert = st.button('Submit Order')
    
    # 5. 注文ボタンが押された場合の処理
    if time_to_insert:
        if name_on_order:
            
            # 注文内容を文字列に変換
            ingredients_string = ', '.join(ingredients_list)
            
            # --- Snowflakeへのデータ挿入処理 ---
            # 次のレッスンで使うための注文データ挿入SQL
            insert_query = f"""
                INSERT INTO smoothies.public.orders (ingredients, name_on_order)
                VALUES ('{ingredients_string}', '{name_on_order}')
            """
            
            # データベースへの挿入実行（本来はtry-exceptでエラー処理をすべき）
            session.sql(insert_query).collect()
            
            # 成功メッセージの表示
            st.success('Your Smoothie is on its way, ' + name_on_order + '!', icon="✅")
        
        else:
            st.warning("Please enter your name before submitting the order.")
