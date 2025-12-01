import streamlit as st
import google.generativeai as genai
import psycopg2
import pandas as pd

st.set_page_config(page_title="SQL Data Assistant", page_icon="📊")

def check_password():
    left, right = st.columns([1, 2])

    with left:
        st.write("### 🔐 Secure Login")

        def password_entered():
            if st.session_state["password"] == st.secrets["general"]["app_password"]:
                st.session_state["password_correct"] = True
                del st.session_state["password"]
            else:
                st.session_state["password_correct"] = False

        if "password_correct" not in st.session_state:
            st.text_input("Enter Password:", type="password",
                          on_change=password_entered, key="password")
            return False
        elif not st.session_state["password_correct"]:
            st.text_input("Incorrect. Try again:", type="password",
                          on_change=password_entered, key="password")
            return False
        else:
            return True

    with right:
        st.image("https://static.vecteezy.com/system/resources/thumbnails/014/440/530/small_2x/security-padlock-icon-in-flat-style-design-illustration-free-vector.jpg",
                 width=250)

if not check_password():
    st.stop()

st.title("🤖 AI Database Assistant")
st.markdown("Ask questions about your data in plain English.")

genai.configure(api_key=st.secrets["google"]["api_key"])

schema_prompt = """
You are a SQL expert. Convert the user's natural language question into a PostgreSQL query.
The database schema is as follows:

Region (RegionID SERIAL PK, Region TEXT);
Country (CountryID SERIAL PK, Country TEXT, RegionID INT FK -> Region);
Customer (CustomerID SERIAL PK, FirstName TEXT, LastName TEXT, Address TEXT, City TEXT, CountryID INT FK -> Country);
ProductCategory (ProductCategoryID SERIAL PK, ProductCategory TEXT, ProductCategoryDescription TEXT);
Product (ProductID SERIAL PK, ProductName TEXT, ProductUnitPrice REAL, ProductCategoryID INT FK -> ProductCategory);
OrderDetail (OrderID SERIAL PK, CustomerID INT FK -> Customer, ProductID INT FK -> Product, OrderDate DATE, QuantityOrdered INT);

RULES:
1. Return ONLY the SQL query.
2. Use valid PostgreSQL syntax.
3. Use ILIKE for text matching.
4. For revenue use (QuantityOrdered * ProductUnitPrice).
5. Join tables correctly.
"""

def get_gemini_sql(question):
    model = genai.GenerativeModel('gemini-2.5-flash')
    prompt = f"{schema_prompt}\n\nUser Question: {question}\nSQL Query:"
    try:
        response = model.generate_content(prompt)
        sql = response.text.strip()
        sql = sql.replace("sql", "").replace("```", "").strip()
        return sql
    except:
        return None

def run_query(sql_query):
    conn = None
    try:
        conn = psycopg2.connect(st.secrets["postgres"]["url"])
        df = pd.read_sql_query(sql_query, conn)
        return df
    except:
        return None
    finally:
        if conn:
            conn.close()

left_col, right_col = st.columns([1, 2])

with left_col:
    user_input = st.text_input("Enter your query:",
                               placeholder="e.g., Show me the top 5 customers by total spending")
    run_button = st.button("Generate & Run Query")

with right_col:
    if run_button:
        if user_input:
            with st.spinner("Generating SQL..."):
                sql_query = get_gemini_sql(user_input)

            if sql_query:
                st.subheader("Generated SQL:")
                st.code(sql_query, language="sql")

                with st.spinner("Fetching data from Database..."):
                    results = run_query(sql_query)

                if results is not None and not results.empty:
                    st.subheader("Results:")
                    st.dataframe(results)
                elif results is not None:
                    st.info("Query ran successfully but returned no results.")
        else:
            st.warning("Please enter a question.")
