import pandas as pd
import streamlit as st
import altair as alt
from data_processing import finalize_data, finalize_data_clel
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import os

# Set the title and favicon that appear in the Browser's tab bar.
st.set_page_config(
    page_title='Kognisi Learners',
    page_icon=':bar_chart:',  # This is an emoji shortcode. Could be a URL too.
)

# Function to log user access to Google Sheets
def log_user_access(email):
    access_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Setup the Google Sheets client
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(st.secrets["json_sap"], scope)
    client = gspread.authorize(creds)
    
    # Open the Google Sheet
    sheet = client.open("Kognisi Dashboard Access Log").sheet1  # Assuming you want to log in the first sheet

    # Log the data
    sheet.append_row([email, access_time])

# Get the user's email from Streamlit's experimental_user function
user_info = st.experimental_user

# Check if user_info contains the email and log access
if user_info is not None and "email" in user_info:
    user_email = user_info['email']
    log_user_access(user_email)
    st.write(f"Welcome, {user_email}!")
else:
    st.write("No email detected. Are you sure you are signed in?")
    st.write("Debug info:", user_info)

# Return data from data_processing
merged_df, df_combined_mysql, df_sap, right_merged_df = finalize_data()
df_clel = finalize_data_clel()

# Add logo and title above sidebar
st.logo('kognisi_logo.png')

# Set the title that appears at the top of the page.
st.markdown('''
# :bar_chart: Kognisi Learners

Under the term Kognisi Learners, there are several metrics, including:
1. **Active Learners:** users who have accessed at least one content in one platform
2. **Collaborative Learners:** active learners who also teach at least one learning content
3. **Exponential Learner:** active learners who teach more than one learning content
''')

# Add some spacing
st.markdown('''

''')

# Create date filter for last_updated
min_value = merged_df['last_updated'].min()
max_value = merged_df['last_updated'].max()

# Initialize session state for date filters if not already present
if 'from_date' not in st.session_state:
    st.session_state.from_date = min_value
if 'to_date' not in st.session_state:
    st.session_state.to_date = max_value

# Default date range
from_date = min_value
to_date = max_value

# Create columns for buttons
st.write("**Choose the data period:**")
col1, col2, col3 = st.columns(3)

# Create buttons for shortcut filters in a single line
with col1:
    if st.button('Lifetime'):
        from_date = min_value
        to_date = max_value
        st.session_state.from_date = from_date
        st.session_state.to_date = to_date

with col2:
    if st.button('This Year'):
        current_year = max_value.year
        from_date = datetime(current_year, 1, 1).date()
        to_date = max_value
        st.session_state.from_date = from_date
        st.session_state.to_date = to_date

with col3:
    if st.button('This Month'):
        current_year = max_value.year
        current_month = max_value.month
        from_date = datetime(current_year, current_month, 1).date()
        to_date = max_value
        st.session_state.from_date = from_date
        st.session_state.to_date = to_date

# Allow manual date input as well
from_date, to_date = st.date_input(
    '**Or pick the date manually:**',
    value=[from_date, to_date],
    min_value=min_value,
    max_value=max_value,
    format="YYYY-MM-DD"
)
st.session_state.from_date = from_date
st.session_state.to_date = to_date

# Filter the data based on the selected date range
filtered_df = merged_df[
    (merged_df['last_updated'] <= to_date) & (merged_df['last_updated'] >= from_date)
]

# Active Learners section
st.header('Active Learners', divider='gray')

# Calculate the distinct counts of the count AL column
total_count = filtered_df['count AL'].nunique()
internal_count = filtered_df[filtered_df['status'] == 'Internal']['count AL'].nunique()
external_count = filtered_df[filtered_df['status'] == 'External']['count AL'].nunique()

# Display metrics column
col1, col2, col3 = st.columns(3)
col1.metric("Overall", total_count)
col2.metric("Internal", internal_count)
col3.metric("External", external_count)

# Platform Distribution
st.subheader('Platform Distribution', divider='gray')

# Group by platform and status, and count unique count ALs
platform_counts = filtered_df.groupby(['platform', 'status'])['count AL'].nunique().reset_index()
platform_counts.columns = ['platform', 'status', 'learners']

# Create Altair bar chart with internal and external status
chart = alt.Chart(platform_counts).mark_bar().encode(
    x=alt.X('platform', sort='-y', axis=alt.Axis(title='Platform')),
    y=alt.Y('learners', axis=alt.Axis(title='Active Learners')),
    color=alt.Color('status', scale=alt.Scale(domain=['Internal', 'External'], range=['#1f77b4', '#ff7f0e'])),
    tooltip=['platform', 'status', 'learners']
).properties(
    width=alt.Step(80)  # Adjust width as needed
)

# Display the chart using Streamlit
st.altair_chart(chart, use_container_width=True)

st.write('***Internal:** PKWT & PKWTT (Registered in SAP) | **Eksternal:** Non KG, Ex-employee, Freelance, Intern*')

# =========================================
# Collaborative & Exponential Learners
# =========================================
st.header('Collaborative & Exponential Learners', divider='gray')

if df_clel is not None and not df_clel.empty:

    clel_filtered = df_clel.copy()

    # =========================================
    # 1️⃣ CL EL JUMLAH
    # =========================================
    pembicara_group = (
        clel_filtered.groupby('id_pembicara')['id_course']
        .nunique()
        .reset_index(name='Jumlah Konten')
    )

    pembicara_group['Kategori'] = pembicara_group['Jumlah Konten'].apply(
        lambda x: 'Exponential' if x > 1 else 'Collaborative'
    )

    total_cl = (pembicara_group['Kategori'] == 'Collaborative').sum()
    total_el = (pembicara_group['Kategori'] == 'Exponential').sum()

    col1, col2 = st.columns(2)
    col1.metric("Collaborative", total_cl)
    col2.metric("Exponential", total_el)
    st.write('***Collaborative** Lifetime learners (Internal + eksternal) yang mengajar 1 Konten | **Exponential:** Lifetime learners (Internal + eksternal) yang mengajar lebih dari 1 Konten*')

# =========================================
# 2️⃣ SUMMARY + INTERNAL BREAKDOWN (2 COLUMN)
# =========================================

if 'User' in clel_filtered.columns and 'Unit (Monthly Updated)' in clel_filtered.columns:

    col1, col2 = st.columns(2)

    # =========================
    # COLUMN 1 → SUMMARY INTERNAL vs EKSTERNAL
    # =========================
    with col1:

        summary = (
            clel_filtered.groupby('User')['id_pembicara']
            .nunique()
            .reset_index(name='Total')
        )

        summary_chart = alt.Chart(summary).mark_bar().encode(
            x=alt.X('User:N'),
            y=alt.Y('Total:Q'),
            color=alt.Color(
                'User:N',
                scale=alt.Scale(
                    domain=['INTERNAL', 'EKSTERNAL'],
                    range=['#1f77b4', '#ff7f0e']
                )
            )
        )

        summary_text = alt.Chart(summary).mark_text(dy=-5).encode(
            x='User:N',
            y='Total:Q',
            text='Total:Q'
        )

        st.subheader("Internal vs Eksternal")
        st.altair_chart(summary_chart + summary_text, use_container_width=True)

    # =========================
    # COLUMN 2 → BREAKDOWN INTERNAL ONLY
    # =========================
    with col2:

        internal_df = clel_filtered[
            clel_filtered['User'] == 'INTERNAL'
        ]

        unit_breakdown = (
            internal_df.groupby('Unit (Monthly Updated)')['id_pembicara']
            .nunique()
            .reset_index(name='Total')
            .sort_values(by='Total', ascending=False)
        )

        unit_chart = alt.Chart(unit_breakdown).mark_bar().encode(
            x=alt.X('Unit (Monthly Updated):N', sort='-y'),
            y=alt.Y('Total:Q'),
            color=alt.value('#1f77b4')
        )

        unit_text = alt.Chart(unit_breakdown).mark_text(
            dy=-5,
            color='black'
        ).encode(
            x='Unit (Monthly Updated):N',
            y='Total:Q',
            text='Total:Q'
        )

        st.subheader("Internal by Unit")
        st.altair_chart(unit_chart + unit_text, use_container_width=True)

    # =========================================
    # 3️⃣ EXPANDER DETAIL PEMBICARA
    # =========================================
    if 'Pembicara' in clel_filtered.columns:

        detail = clel_filtered[['Pembicara', 'id_pembicara']].drop_duplicates()
        detail = detail.merge(pembicara_group, on='id_pembicara', how='left')

        with st.expander("Lihat Detail Pembicara (Collaborative / Exponential)"):
            st.dataframe(
                detail.sort_values(by='Jumlah Konten', ascending=False),
                use_container_width=True
            )

else:
    st.info('No CL EL data available')









# Identify columns to keep: id_instructor and columns starting with 'EL/CL'
#columns_to_keep = ['id_instructor'] + [col for col in df_clel.columns if col.startswith('EL/CL')]
#df_clel_trim = df_clel[columns_to_keep]

# Melt the DataFrame to long format
#df_melted = df_clel_trim.melt(id_vars=['id_instructor'], var_name='month', value_name='value')

# Rename the month columns to be more readable
#df_melted['month'] = df_melted['month'].str.replace('EL/CL ', '')

# Count unique IDs for each value per month
#count_data = df_melted.groupby(['month', 'value']).agg({'id_instructor': pd.Series.nunique}).reset_index()
#count_data.rename(columns={'id_instructor': 'unique_count'}, inplace=True)

# Filter only EL and CL values
#count_data = count_data[count_data['value'].isin(['EL', 'CL'])]

# Define a list of months in order
#standard_months = ['JANUARY', 'FEBRUARY', 'MARCH', 'APRIL', 'MAY', 'JUNE', 'JULY', 'AUGUST', 'SEPTEMBER', 'OCTOBER', 'NOVEMBER', 'DECEMBER']

# Identify months present in the data
#present_months = count_data['month'].unique()

# Combine standard months with any new months found in the data
#month_order = [month for month in standard_months if month in present_months] + \
              #[month for month in present_months if month not in standard_months]

# Ensure the month is categorical and in the specified order
#count_data['month'] = pd.Categorical(count_data['month'], categories=month_order, ordered=True)
#count_data = count_data.sort_values(by='month')

# Create the chart using Altair
#chart = alt.Chart(count_data).mark_bar().encode(
    #x=alt.X('month', sort=None, axis=alt.Axis(title='Month')),
    #y=alt.Y('unique_count', axis=alt.Axis(title='Count')),
    #color=alt.Color('value:N', scale=alt.Scale(domain=['CL', 'EL'], range=['#ADD8E6', '#40E0D0'])),  # Light Blue and Turquoise Green
    #order=alt.Order('value:N', sort='ascending'),
    #tooltip=['month', 'value', 'unique_count']
#).properties(
    #width=alt.Step(80)  # Adjust width as needed
#)

#st.altair_chart(chart, use_container_width=True)

# View Data
#st.markdown('#### View Data')

# Get unique values for name and title columns
#unique_df = df_clel[['instructor_name', 'last_updated', 'unit', 'title', 'type', 'platform']].drop_duplicates()

# Filter unit
#unit_list = ['All'] + list(unique_df['unit'].unique())
#selected_unit = st.selectbox('Select Unit:', unit_list)

#if selected_unit != 'All':
    #unique_df = unique_df[unique_df['unit'] == selected_unit]

# Set the default date range based on the data in another_df
#min_value_2 = unique_df['last_updated'].min()
#max_value_2 = unique_df['last_updated'].max()

# Allow manual date input for the second DataFrame
#from_date_2, to_date_2 = st.date_input(
    #'Select date:',
    #value=[min_value_2, max_value_2],
    #min_value=min_value_2,
    #max_value=max_value_2,
    #format="YYYY-MM-DD"
#)

# Filter the second DataFrame based on the selected date range
#unique_df = unique_df[
    #(unique_df['last_updated'] <= to_date_2) & (unique_df['last_updated'] >= from_date_2)
#]

# Display data
#with st.expander("Collaborative & Exponential Learners"):
    #st.dataframe(unique_df)


# Update Data
st.divider()
st.markdown('''
_This app is using data cache for performance optimization, you can reload the data by clicking the button below then press 'R' on keyboard or refresh the page._
''')
if st.button("Update Data"):
    # Clear values from *all* all in-memory and on-disk data caches:
    st.cache_resource.clear()
    st.cache_data.clear()
