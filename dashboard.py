import streamlit as st
import pandas as pd
from collections import Counter
import ast
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import snowflake.connector
import base64



# --- Snowflake Connection ---
@st.cache_resource
def connect_to_snowflake():
    # Decode base64-encoded private key from secrets
    private_key_b64 = st.secrets["SNOWFLAKE_PRIVATE_KEY"]
    private_key_der = base64.b64decode(private_key_b64)

    # Load the private key in DER format
    private_key = serialization.load_der_private_key(
        private_key_der,
        password=None,
        backend=default_backend()
    )

    # Convert to the format expected by Snowflake connector
    private_key_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

    return snowflake.connector.connect(
        user=st.secrets["SNOWFLAKE_USER"],
        private_key=private_key_bytes,
        account=st.secrets["SNOWFLAKE_ACCOUNT"],
        warehouse=st.secrets["SNOWFLAKE_WAREHOUSE"],
        database=st.secrets["SNOWFLAKE_DATABASE"],
        schema=st.secrets["SNOWFLAKE_SCHEMA"],
        role=st.secrets["SNOWFLAKE_ROLE"]
    )

conn = connect_to_snowflake()


# --- Data Fetching ---
@st.cache_data(ttl=300)
def fetch_data():
    query = "SELECT * FROM TECH_NEWS_EVENTS ORDER BY TIMESTAMP DESC LIMIT 1000"
    return pd.read_sql(query, conn)

df = fetch_data()

# --- Handle Empty Dataset ---
if df.empty:
    st.warning("No recent events found in the database.")
    st.stop()

# --- App Layout ---
st.title("📰 Tech Industry Intelligence Hub")
st.write("Real-time insights from streaming tech news")

# --- Timeline of News Mentions ---
st.subheader("🕒 Timeline of News Mentions")
df['HOUR'] = pd.to_datetime(df['TIMESTAMP']).dt.floor('H')
timeline_counts = df['HOUR'].value_counts().sort_index()
st.line_chart(timeline_counts)

# --- Most Mentioned Companies ---
# Top Companies
st.subheader("🏢 Most Mentioned Companies")
all_companies = []

for company_entry in df['COMPANY']:
    if isinstance(company_entry, str):
        # If it's a list as string, parse it
        if company_entry.startswith("[") and company_entry.endswith("]"):
            try:
                parsed = ast.literal_eval(company_entry)
                all_companies.extend(parsed)
            except:
                all_companies.append(company_entry)
        else:
            all_companies.append(company_entry)

top_companies = Counter(all_companies).most_common(10)
top_companies_df = pd.DataFrame(top_companies, columns=["Company", "Mentions"])
st.bar_chart(top_companies_df.set_index("Company"))


# --- Top Keywords / Topics ---
st.subheader("🔑 Top Keywords / Topics")
all_topics = []
for topics in df['TOPIC']:
    try:
        all_topics.extend(ast.literal_eval(topics))
    except:
        continue

top_topics = Counter(all_topics).most_common(10)
top_topics_df = pd.DataFrame(top_topics, columns=["Keyword", "Count"])
st.bar_chart(top_topics_df.set_index("Keyword"))

# --- Raw Data Preview ---
with st.expander("🔍 View Raw Data"):
    st.dataframe(df.sort_values("TIMESTAMP", ascending=False))
