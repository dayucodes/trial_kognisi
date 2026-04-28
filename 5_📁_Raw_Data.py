import streamlit as st
import pandas as pd
import altair as alt
from data_processing import finalize_data
import datetime
import numpy as np

# Set the title and favicon for the browser's tab bar
st.set_page_config(page_title='Learning Hours', page_icon=':hourglass:')

# Add logo and title above the sidebar
st.logo('kognisi_logo.png')

# Fetch the data
merged_df, df_combined_mysql, df_sap, right_merged_df = finalize_data()

# Set the title that appears at the top of the page
st.markdown('''
            # :hourglass: Learning Hours
            
            This page provides insights into how many employees achieved the target learning hours per unit.
            ''')

# Sidebar: Unit Filter
st.sidebar.markdown('### Unit Filter')
unit_list = list(df_sap['unit'].dropna().unique())
selected_units = st.sidebar.multiselect('Select Unit(s):', unit_list, default=[])

if selected_units:
    df_sap = df_sap[df_sap['unit'].isin(selected_units)]

# Penugasan Filter
penugasan_list = list(df_sap['penugasan'].dropna().unique())
selected_penugasan = st.sidebar.multiselect('Select Penugasan:', penugasan_list, default=[])

if selected_penugasan:
    df_sap = df_sap[df_sap['penugasan'].isin(selected_penugasan)]

# Admin GOMAN Filter (only shown if 'GOMAN' is in selected_units)
if 'GOMAN' in selected_units:
    admin_goman_list = list(df_sap['admin_goman'].dropna().unique())
    selected_admin_goman = st.sidebar.multiselect('Select Admin GOMAN:', admin_goman_list, default=[])

    if selected_admin_goman:
        df_sap = df_sap[df_sap['admin_goman'].isin(selected_admin_goman)]

# Subunit Filter (only for KG MEDIA, GOMAN, YMN)
if any(unit in ['KG MEDIA', 'GOMAN', 'YMN'] for unit in selected_units):
    subunit_list = list(df_sap['subunit'].dropna().unique())
    selected_subunit = st.sidebar.multiselect('Select Subunit:', subunit_list, default=[])

    if selected_subunit:
        df_sap = df_sap[df_sap['subunit'].isin(selected_subunit)]

selected_division = st.sidebar.multiselect('Select Division:', list(df_sap['division'].unique()), default=[])
if selected_division:
    df_sap = df_sap[df_sap['division'].isin(selected_division)]

selected_layer = st.sidebar.multiselect('Select Layer:', list(df_sap['layer'].unique()), default=[])
if selected_layer:
    df_sap = df_sap[df_sap['layer'].isin(selected_layer)]

selected_region = st.sidebar.multiselect('Select Region:', list(df_sap['region'].unique()), default=[])
if selected_region:
    df_sap = df_sap[df_sap['region'].isin(selected_region)]

# Sidebar: Content Filter
st.sidebar.markdown('### Content Filter')
category_list = list(df_combined_mysql['category'].dropna().unique())
selected_category = st.sidebar.multiselect('Select Category:', category_list, default=[])

if selected_category:
    df_combined_mysql = df_combined_mysql[df_combined_mysql['category'].isin(selected_category)]

# Sidebar: Breakdown variable
st.sidebar.markdown('### Breakdown Variable')
breakdown_variable = st.sidebar.selectbox('Select Breakdown Variable:', 
                                          ['unit', 'subunit', 'layer', 'generation', 'gender', 'division', 'department', 'admin_goman'])

# Create date filter for last_updated
min_value = df_combined_mysql['last_updated'].min()
max_value = df_combined_mysql['last_updated'].max()

# Create date filter for last_updated
min_value = df_combined_mysql['last_updated'].min()
max_value = df_combined_mysql['last_updated'].max()

# Page-specific session state keys
FROM_KEY = "lh_from_date"
TO_KEY = "lh_to_date"

# Default = this year (based on max_value)
current_year = max_value.year
default_from = datetime.date(current_year, 1, 1)
default_to = max_value

# Clamp if dataset starts after Jan 1
if default_from < min_value:
    default_from = min_value

# Initialize session state for this page only
if FROM_KEY not in st.session_state:
    st.session_state[FROM_KEY] = default_from
if TO_KEY not in st.session_state:
    st.session_state[TO_KEY] = default_to

# Default date range (from session)
from_date = st.session_state[FROM_KEY]
to_date = st.session_state[TO_KEY]

# Create columns for buttons
st.write("**Choose the data period:**")
col1, col2, col3 = st.columns(3)

# Create buttons for shortcut filters in a single line
with col1:
    if st.button('Lifetime'):
        from_date = min_value
        to_date = max_value
        st.session_state[FROM_KEY] = from_date
        st.session_state[TO_KEY] = to_date

with col2:
    if st.button('This Year'):
        current_year = max_value.year
        from_date = datetime.date(current_year, 1, 1)
        to_date = max_value
        st.session_state[FROM_KEY] = from_date
        st.session_state[TO_KEY] = to_date

with col3:
    if st.button('This Month'):
        current_year = max_value.year
        current_month = max_value.month
        from_date = datetime.date(current_year, current_month, 1)
        to_date = max_value
        st.session_state[FROM_KEY] = from_date
        st.session_state[TO_KEY] = to_date

# Allow manual date input as well
from_date, to_date = st.date_input(
    '**Or pick the date manually:**',
    value=[from_date, to_date],
    min_value=min_value,
    max_value=max_value,
    format="YYYY-MM-DD"
)

# Update session state with manually picked dates
st.session_state[FROM_KEY] = from_date
st.session_state[TO_KEY] = to_date

# Filter the data based on the selected date range
df_combined_mysql = df_combined_mysql[
    (df_combined_mysql['last_updated'] <= to_date) & (df_combined_mysql['last_updated'] >= from_date)
]

# Calculate months in range and dynamic target learning hours
def calculate_months(start_date, end_date):
    return (end_date.year - start_date.year) * 12 + end_date.month - start_date.month + 1

months_in_range = calculate_months(pd.to_datetime(from_date), pd.to_datetime(to_date))
#target_hours = months_in_range

# Dictionary to store target multipliers for each unit
#unit_targets = {
#    'GOMAN': 0.5,
#    # Add more units and their multipliers as needed
#}
def month_start(d: pd.Timestamp) -> pd.Timestamp:
    return pd.Timestamp(year=d.year, month=d.month, day=1)

def iter_month_starts(start: pd.Timestamp, end: pd.Timestamp):
    """Yield month-start timestamps from start..end (inclusive), both normalized to month starts."""
    cur = month_start(start)
    last = month_start(end)
    while cur <= last:
        yield cur
        # next month
        cur = (cur + pd.offsets.MonthBegin(1)).normalize()

def base_hours_per_month(month_ts: pd.Timestamp, unit: str) -> float:
    unit = (unit or "").strip().upper()

    # GOMAN stays 0.5 hour/month forever
    if unit == "GOMAN":
        return 0.5

    # others: starting from 2026 -> 1.5 hours per month
    return 1.5 if month_ts.year >= 2026 else 1.0

def get_target_hours(row, from_date, to_date):
    effective_start = max(pd.to_datetime(from_date), pd.to_datetime(row['join_date']))
    effective_end = pd.to_datetime(to_date)

    if effective_start > effective_end:
        return 0.0

    unit = row.get("unit", "")

    total = 0.0
    for m in iter_month_starts(effective_start, effective_end):
        total += base_hours_per_month(m, unit)

    return total

# Merge with SAP data
learning_hours = pd.merge(df_combined_mysql, df_sap, left_on='count AL', right_on='nik', how='right')

# Convert duration from seconds to hours and sum the duration hours
learning_hours['duration_hours'] = learning_hours['duration'] / 3600
learning_hours['duration_hours'] = learning_hours['duration_hours'].astype(float)
learning_hours['total_hours'] = learning_hours.groupby('count AL')['duration_hours'].transform('sum')

# Apply
learning_hours['target_hours'] = learning_hours.apply(
    lambda row: get_target_hours(row, from_date, to_date), axis=1
)

# Determine whether each employee achieved the target
learning_hours['achieved_target'] = np.where(
    learning_hours['total_hours'].isna(), 'Inactive',
    np.where(learning_hours['total_hours'] >= learning_hours['target_hours'], 'Achieved', 'Not Achieved')
)

# Aggregate data by unit
unit_achievement = learning_hours.pivot_table(index=breakdown_variable, values='nik_y', columns='achieved_target', 
                                              aggfunc='nunique', fill_value=0).reset_index()

# Ensure both 'Achieved' and 'Not Achieved' columns exist
if 'Achieved' not in unit_achievement:
    unit_achievement['Achieved'] = 0
if 'Inactive' not in unit_achievement:
    unit_achievement['Inactive'] = 0
if 'Not Achieved' not in unit_achievement:
    unit_achievement['Not Achieved'] = 0

# Normalize counts for 100% stacked bar chart
unit_achievement['Achieved (%)'] = unit_achievement['Achieved'] / (unit_achievement['Achieved'] + unit_achievement['Not Achieved'] + unit_achievement['Inactive']) * 100
unit_achievement['Not Achieved (%)'] = unit_achievement['Not Achieved'] / (unit_achievement['Achieved'] + unit_achievement['Not Achieved'] + unit_achievement['Inactive']) * 100
unit_achievement['Inactive (%)'] = unit_achievement['Inactive'] / (unit_achievement['Achieved'] + unit_achievement['Not Achieved'] + unit_achievement['Inactive']) * 100

# Transform data for Altair
melted_counts = unit_achievement.melt(
    id_vars=breakdown_variable,
    value_vars=['Achieved', 'Not Achieved', 'Inactive'],
    var_name='Achievement',
    value_name='Count'
)

melted_percentage = unit_achievement.melt(
    id_vars=breakdown_variable,
    value_vars=['Achieved (%)', 'Not Achieved (%)', 'Inactive (%)'],
    var_name='Achievement',
    value_name='Percent'
)

# Combine counts and percentage into a single DataFrame
melted_counts['Percent'] = melted_percentage['Percent']

# Calculate unique learning hours per 'nik_y'
unique_learning_hours = learning_hours.drop_duplicates(subset=['nik_y'])

# Calculate summary statistics
total_employees = df_sap['nik'].nunique()
achieved_employees = unit_achievement['Achieved'].sum()
percent_achieved = (achieved_employees / total_employees) * 100

# Calculate average hours per active employee (with total_hours >= 0) and per all employees based on unique 'nik_y'
average_hours_active = unique_learning_hours[unique_learning_hours['total_hours'] >= 0]['total_hours'].mean()
average_hours_all = unique_learning_hours['total_hours'].sum() / total_employees

st.write('## Summary:')
st.markdown(f'- **Total Employees**: {total_employees}')
st.markdown(f'- **Employees Achieved Target**: {achieved_employees} ({percent_achieved:.2f}%)')
st.markdown(f'- **Avg. Hours per Active Employee**: {average_hours_active:.1f}')
st.markdown(f'- **Avg. Hours per All Employee**: {average_hours_all:.1f}')

# Display the calculated data as a horizontal 100% stacked bar chart
st.header(f'Learning Hours Achievement by {breakdown_variable.capitalize()}', divider='gray')

# Create the 100% stacked bar chart
chart = alt.Chart(melted_counts).mark_bar().encode(
    y=alt.Y(f'{breakdown_variable}:N', sort=None, axis=alt.Axis(title=breakdown_variable.capitalize())),
    x=alt.X('Percent:Q', axis=alt.Axis(title='Percentage'), scale=alt.Scale(domain=[0, 100])),
    color=alt.Color('Achievement:N', scale=alt.Scale(domain=['Achieved', 'Not Achieved', 'Inactive'], range=['#1f77b4', '#ff7f0e', '#808080'])),
    order=alt.Order('index:Q', sort='ascending'),    # Ensure active is plotted first    
    tooltip=[
        alt.Tooltip(f'{breakdown_variable}:N', title=breakdown_variable.capitalize()),
        alt.Tooltip('Achievement:N', title='Achievement'),
        alt.Tooltip('Count:Q', title='Count'),
        alt.Tooltip('Percent:Q', title='Percentage', format='.1f')
    ]
).properties(
    width=800  # Adjust width as needed
)

# Display the chart using Streamlit
st.altair_chart(chart, use_container_width=True)

# Display final_counts
with st.expander('Data Source'):
    st.dataframe(unit_achievement)

# Display the raw data
st.header('Download Data', divider='gray')

# Define the columns to drop from df_combined_mysql
columns_drop = ['email_x', 'name', 'nik_x', 'title', 'last_updated', 'duration', 'type', 'platform', 'count AL', 'duration_hours', 'progress', 'category', 'admin_goman', 'penugasan']  # replace with actual columns to drop
unique_LH = learning_hours.drop(columns=columns_drop, errors='ignore').drop_duplicates()

# Section for Achieved Learners
with st.expander("Achieved"):
    achieved_df = unique_LH[unique_LH['achieved_target'] == 'Achieved']
    achieved_df.index = range(1, len(achieved_df) + 1)
    st.dataframe(achieved_df)

# Section for Not Acvhieved Learners
with st.expander("Not Achieved"):
    nachived_df = unique_LH[unique_LH['achieved_target'] == 'Not Achieved']
    nachived_df.index = range(1, len(nachived_df) + 1)
    st.dataframe(nachived_df)

# Section for Inactive
with st.expander("Inactive"):
    inactive_df = unique_LH[unique_LH['achieved_target'] == 'Inactive']
    inactive_df.index = range(1, len(inactive_df) + 1)
    st.dataframe(inactive_df)

# Download data
# Convert the DataFrame to CSV
csv = unique_LH.to_csv(index=False)

# Create a download button
st.download_button(
    label="Download full data as CSV",
    data=csv,
    file_name='full_data_LH.csv',
    mime='text/csv',
)

# Update Data
st.divider()
st.markdown('''
_This app is using data cache for performance optimization, you can reload the data by clicking the button below then press 'R' on keyboard or refresh the page._
''')
if st.button("Update Data"):
    # Clear values from *all* all in-memory and on-disk data caches:
    st.cache_resource.clear()
    st.cache_data.clear()
