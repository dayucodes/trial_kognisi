import streamlit as st
import pandas as pd
import altair as alt
from data_processing import finalize_data

# Set the title and favicon for the Browser's tab bar.
st.set_page_config(
    page_title='Learning Adoption',
    page_icon=':brain:',  # This is an emoji shortcode. Could be a URL too.
)

# Add logo and title above sidebar
st.logo('kognisi_logo.png')

# Fetch the data
merged_df, df_combined_mysql, df_sap, right_merged_df = finalize_data()

# Sidebar: Unit Filter
st.sidebar.markdown('### Unit Filter')
unit_list = list(right_merged_df['unit'].dropna().unique())
selected_units = st.sidebar.multiselect('Select Unit(s):', unit_list, default=[])

if selected_units:
    right_merged_df = right_merged_df[right_merged_df['unit'].isin(selected_units)]

# Penugasan Filter
penugasan_list = list(right_merged_df['penugasan'].dropna().unique())
selected_penugasan = st.sidebar.multiselect('Select Penugasan:', penugasan_list, default=[])

if selected_penugasan:
    right_merged_df = right_merged_df[right_merged_df['penugasan'].isin(selected_penugasan)]

# Admin GOMAN Filter (only shown if GOMAN is in selected_units)
if 'GOMAN' in selected_units:
    admin_goman_list = list(right_merged_df['admin_goman'].dropna().unique())
    selected_admin_goman = st.sidebar.multiselect('Select Admin GOMAN:', admin_goman_list, default=[])

    if selected_admin_goman:
        right_merged_df = right_merged_df[right_merged_df['admin_goman'].isin(selected_admin_goman)]

# Subunit filter for specific units
if any(unit in ['KG MEDIA', 'GOMAN', 'YMN'] for unit in selected_units):
    subunit_list = list(right_merged_df['subunit'].dropna().unique())
    selected_subunit = st.sidebar.multiselect('Select Subunit:', subunit_list, default=[])

    if selected_subunit:
        right_merged_df = right_merged_df[right_merged_df['subunit'].isin(selected_subunit)]

division_list = list(right_merged_df['division'].unique())
selected_division = st.sidebar.multiselect('Select Division:', division_list, default=[])

if selected_division:
    right_merged_df = right_merged_df[right_merged_df['division'].isin(selected_division)] 

layer_list = list(right_merged_df['layer'].unique())
selected_layer = st.sidebar.multiselect('Select Layer:', layer_list, default=[])

if selected_layer:
    right_merged_df = right_merged_df[right_merged_df['layer'].isin(selected_layer)]

region_list = list(right_merged_df['region'].unique())
selected_region = st.sidebar.multiselect('Select Region:', region_list, default=[])

if selected_region:
    right_merged_df = right_merged_df[right_merged_df['region'].isin(selected_region)] 

# Sidebar: Add a selectbox for breakdown variable
st.sidebar.markdown ('### Breakdown Variable')
breakdown_variable = st.sidebar.selectbox('Select Breakdown Variable:', ['unit', 'subunit', 'layer', 'generation', 'gender', 'division', 'department', 'region', 'admin_goman'])

# Create pivot table
final_counts = right_merged_df.pivot_table(index=breakdown_variable, columns='status', values='nik_y', aggfunc='nunique', fill_value=0).reset_index()

# Ensure both 'Active Learners' and 'Passive Learners' columns exist
if 'Active' not in final_counts:
    final_counts['Active'] = 0
if 'Passive' not in final_counts:
    final_counts['Passive'] = 0

final_counts.columns = [breakdown_variable, 'Active Learners', 'Passive Learners']

# Calculate Active Learners (%) and Passive Learners (%)
final_counts['Active Learners (%)'] = final_counts['Active Learners'] / (final_counts['Active Learners'] + final_counts['Passive Learners']) * 100
final_counts['Passive Learners (%)'] = final_counts['Passive Learners'] / (final_counts['Active Learners'] + final_counts['Passive Learners']) * 100

# Set the title that appears at the top of the page.
st.markdown('''
# :brain: Learning Adoption

This page provides insights into the adoption of Kognisi learning platforms.
''')

# Display the calculated overall learning adoption
st.header('Overall Learning Adoption', divider='gray')

# Calculate overall learning adoption
total_active_learners = final_counts['Active Learners'].sum()
total_learners = final_counts['Active Learners'].sum() + final_counts['Passive Learners'].sum()
overall_adoption = (total_active_learners / total_learners) * 100

# Display metrics column
col1, col2, col3 = st.columns(3)
col1.metric("Active Learners", int(total_active_learners))
col2.metric("Active Employees", int(total_learners))
col3.metric("Learning Adoption", f"{overall_adoption:.2f}%")

# Display the calculated percentage as a bar chart
st.header(f'Learning Adoption by {breakdown_variable.capitalize()}', divider='gray')

# Transform data for Altair
melted_counts = final_counts.melt(
    id_vars=breakdown_variable,
    value_vars=['Active Learners', 'Passive Learners'],
    var_name='Learner Type',
    value_name='Count'
)

melted_percentage = final_counts.melt(
    id_vars=breakdown_variable,
    value_vars=['Active Learners (%)', 'Passive Learners (%)'],
    var_name='Learner Type',
    value_name='Percent'
)

# Combine counts and percentage into a single DataFrame
melted_counts['Percent'] = melted_percentage['Percent']

# Create the base chart with Active and Passive Learners
base = alt.Chart(melted_counts).mark_bar().encode(
    x=alt.X(f'{breakdown_variable}:N', sort='-y', axis=alt.Axis(title=breakdown_variable.capitalize())),
    y=alt.Y('Percent:Q', axis=alt.Axis(title='Learning Adoption (%)'), scale=alt.Scale(domain=[0, 100])),
    color=alt.Color('Learner Type:N', scale=alt.Scale(domain=['Active Learners', 'Passive Learners'], range=['#1f77b4', '#ff7f0e'])),
    order=alt.Order('Learner Type:N', sort='ascending'),    # Ensure active is plotted first
    tooltip=[
        alt.Tooltip(f'{breakdown_variable}:N', title=breakdown_variable.capitalize()),
        alt.Tooltip('Learner Type:N', title='Learner Type'),
        alt.Tooltip('Count:Q', title='Count'),
        alt.Tooltip('Percent:Q', title='Percentage', format='.1f')
    ]
).properties(
    width=alt.Step(40)   # Adjust width as needed
)

# Add labels only for Active Learners with black text
text = base.mark_text(
    align='center',
    baseline='middle',
    dy=20,  # Adjust label position above the bars
    fontWeight='bold'  # Make the text bold
).encode(
    text=alt.Text('Percent:Q', format='.0f'),  # Display percentage without decimals
    color=alt.value('black')  # Explicitly set the text color to black
).transform_filter(
    alt.FieldEqualPredicate(field='Learner Type', equal='Active Learners')  # Only label Active Learners
)

# Add the Learning Adoption line chart
line = alt.Chart(final_counts).mark_line(color='green').encode(
    x=f'{breakdown_variable}:N',
    y=alt.Y('Active Learners (%):Q', axis=alt.Axis(title='Learning Adoption (%)'), scale=alt.Scale(domain=[0, 100])),  # Removing the title and setting the scale
    tooltip=[
        alt.Tooltip(f'{breakdown_variable}:N', title=breakdown_variable.capitalize()),
        alt.Tooltip('Active Learners (%):Q', title='Learning Adoption', format='.1f')
    ]
)

# Combine the charts
combo_chart = alt.layer(base, text, line).resolve_scale(
    y='shared'  # Use shared scale for both charts
).properties(
    width=alt.Step(40)  # Adjust width as needed
)

# Display the chart using Streamlit
st.altair_chart(combo_chart, use_container_width=True)

# Display final_counts
with st.expander('Data Source'):
    st.dataframe(final_counts)

# Display the raw data
st.header('Download Data', divider='gray')

# Define the columns to drop from df_combined_mysql
columns_to_drop = ['email_x', 'name', 'nik_x', 'title', 'last_updated', 'duration', 'type', 'platform', 'count AL', 'progress', 'category', 'admin_goman', 'penugasan']  # replace with actual columns to drop
unique_sap_rows = right_merged_df.drop(columns=columns_to_drop, errors='ignore').drop_duplicates()

# Filter and index once
active_learners = unique_sap_rows[unique_sap_rows['status'] == 'Active'].reset_index(drop=True)
passive_learners = unique_sap_rows[unique_sap_rows['status'] == 'Passive'].reset_index(drop=True)

# Add indices for display
active_learners.index += 1
passive_learners.index += 1

# Display data
with st.expander("Active Learners"):
    st.dataframe(active_learners)

with st.expander("Passive Learners"):
    st.dataframe(passive_learners)

# Update Data
st.divider()
st.markdown('''
_This app is using data cache for performance optimization, you can reload the data by clicking the button below then press 'R' on keyboard or refresh the page._
''')
if st.button("Update Data"):
    # Clear values from *all* all in-memory and on-disk data caches:
    st.cache_resource.clear()
    st.cache_data.clear()
