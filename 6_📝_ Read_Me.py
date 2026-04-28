import streamlit as st
import pandas as pd
import datetime
from data_processing import finalize_data

# Set the title and favicon for the Browser's tab bar.
st.set_page_config(
    page_title='Top Contents',
    page_icon=':trophy:',  # This is an emoji shortcode. Could be a URL too.
)

# Add logo and title above sidebar
st.logo('kognisi_logo.png')

# Fetch the data
merged_df, df_combined_mysql, df_sap, right_merged_df = finalize_data()

# Sidebar: Unit filter
st.sidebar.markdown('### Unit Filter')
unit_list = ['All'] + list(merged_df['unit'].unique())
unit_list = list(merged_df['unit'].dropna().unique())
selected_units = st.sidebar.multiselect('Select Unit(s):', unit_list, default=[])

if selected_units:
    merged_df = merged_df[merged_df['unit'].isin(selected_units)]

# Penugasan Filter
penugasan_list = list(merged_df['penugasan'].dropna().unique())
selected_penugasan = st.sidebar.multiselect('Select Penugasan:', penugasan_list, default=[])

if selected_penugasan:
    merged_df = merged_df[merged_df['penugasan'].isin(selected_penugasan)]

# Admin GOMAN Filter (only shown if GOMAN is in selected_units)
if 'GOMAN' in selected_units:
    admin_goman_list = list(merged_df['admin_goman'].dropna().unique())
    selected_admin_goman = st.sidebar.multiselect('Select Admin GOMAN:', admin_goman_list, default=[])

    if selected_admin_goman:
        merged_df = merged_df[merged_df['admin_goman'].isin(selected_admin_goman)]

# Subunit filter for specific units
if any(unit in ['KG MEDIA', 'GOMAN', 'YMN'] for unit in selected_units):
    subunit_list = list(merged_df['subunit'].dropna().unique())
    selected_subunit = st.sidebar.multiselect('Select Subunit:', subunit_list, default=[])

    if selected_subunit:
        merged_df = merged_df[merged_df['subunit'].isin(selected_subunit)]

selected_division = st.sidebar.multiselect('Select Division:', list(merged_df['division'].unique()), default=[])
if selected_division:
    merged_df = merged_df[merged_df['division'].isin(selected_division)]

selected_layer = st.sidebar.multiselect('Select Layer:', list(merged_df['layer'].unique()), default=[])
if selected_layer:
    merged_df = merged_df[merged_df['layer'].isin(selected_layer)]

selected_region = st.sidebar.multiselect('Select Region:', list(merged_df['region'].unique()), default=[])
if selected_region:
    merged_df = merged_df[merged_df['region'].isin(selected_region)]

# Sidebar: Add a selectbox for type filter
st.sidebar.markdown('### Content Filter')
platform_list = ['All'] + list(merged_df['platform'].unique())  # Replace 'type' with the actual column name
selected_platform = st.sidebar.selectbox('Select Platform:', platform_list)

# Apply type filter if a specific type is selected
if selected_platform != 'All':
    merged_df = merged_df[merged_df['platform'] == selected_platform]

type_list = ['All'] + list(merged_df['type'].unique())  # Replace 'type' with the actual column name
selected_type = st.sidebar.selectbox('Select Type:', type_list)

# Apply type filter if a specific type is selected
if selected_type != 'All':
    merged_df = merged_df[merged_df['type'] == selected_type]

# Set the title of the page
st.markdown('''
# :trophy: Top Contents

This page shows the leaderboard of 10 learning contents with most learners.
''')

# Create date filter for end_date
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
        from_date = datetime.date(current_year, 1, 1)
        to_date = max_value
        st.session_state.from_date = from_date
        st.session_state.to_date = to_date

with col3:
    if st.button('This Month'):
        current_year = max_value.year
        current_month = max_value.month
        from_date = datetime.date(current_year, current_month, 1)
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

# Calculate the leaderboard data based on the filtered data
leaderboard = filtered_df.groupby('title')['count AL'].nunique().reset_index()
leaderboard.columns = ['Title', 'Learners']
leaderboard = leaderboard.sort_values(by='Learners', ascending=False).reset_index(drop=True)

# Select only the top 10 most-watched videos
leaderboard = leaderboard.head(10)

# Add ranking column starting from 1 to 10
leaderboard['Rank'] = leaderboard.index + 1

# Rearrange columns to show rank first
leaderboard = leaderboard[['Rank', 'Title', 'Learners']]

# Display the leaderboard table without index
st.table(leaderboard.set_index('Rank'))

# Update Data
st.divider()
st.markdown('''
_This app is using data cache for performance optimization, you can reload the data by clicking the button below then press 'R' on keyboard or refresh the page._
''')
if st.button("Update Data"):
    # Clear values from *all* all in-memory and on-disk data caches:
    st.cache_resource.clear()
    st.cache_data.clear()
