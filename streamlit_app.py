import streamlit as st
import google.generativeai as genai
import psycopg2
import pandas as pd

# 1. Page Configuration
st.set_page_config(page_title="SQL Data Assistant", page_icon="📊")

# 2. Authentication Check
def check_password():
    """Returns True if the user had the correct password."""

    def password_entered():
        """Checks whether a password entered by the user is correct."""
        if st.session_state["password"] == st.secrets["general"]["app_password"]:
            st.session_state["password_correct"] = True
            del st.session_state["password"]  # Don't store password
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        # First run, show input for password
        st.text_input(
            "Please enter the app password:", type="password", on_change=password_entered, key="password"
        )
        return False
    elif not st.session_state["password_correct"]:
        # Password incorrect, show input again
        st.text_input(
            "Password incorrect. Try again:", type="password", on_change=password_entered, key="password"
        )
        return False
    else:
        # Password correct
        return True

if not check_password():
    st.stop()  # Do not run the rest of the app if not authenticated

# --- MAIN APP STARTS HERE ---

st.title(" AI Query Assistant")
st.markdown("Ask questions about your data in plain English.")

# 3. Configure Google Gemini
genai.configure(api_key=st.secrets["google"]["api_key"])

# 4. Define the Schema for the AI
# I have updated OrderDate to DATE based on your previous python code insertion.
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
1. Return ONLY the SQL query. Do not add markdown like sql or explanations.
2. Use valid PostgreSQL syntax.
3. For text matching, use ILIKE for case-insensitivity (e.g., Country ILIKE '%USA%').
4. If the question asks for sales or revenue, calculate it as (QuantityOrdered * ProductUnitPrice).
5. Join tables correctly using the Foreign Keys provided.
"""

def get_gemini_sql(question):
    """Sends user question to Gemini and gets SQL back."""
    model = genai.GenerativeModel('gemini-2.5-flash')
    prompt = f"{schema_prompt}\n\nUser Question: {question}\nSQL Query:"
    
    try:
        response = model.generate_content(prompt)
        sql = response.text.strip()
        # Clean up if Gemini adds markdown formatting
        sql = sql.replace("sql", "").replace("```", "").strip()
        return sql
    except Exception as e:
        st.error(f"Error connecting to AI: {e}")
        return None

def run_query(sql_query):
    """Connects to Render Postgres DB and runs the query."""
    conn = None
    try:
        # Connect using the URL from secrets
        conn = psycopg2.connect(st.secrets["postgres"]["url"])
        
        # Load directly into a Pandas DataFrame
        df = pd.read_sql_query(sql_query, conn)
        return df
    except Exception as e:
        st.error(f"Database Error: {e}")
        return None
    finally:
        if conn:
            conn.close()

# 5. UI Elements
user_input = st.text_input("Enter your query:", placeholder="e.g., Show me the top 5 customers by total spending")

if st.button("Generate & Run Query"):
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
