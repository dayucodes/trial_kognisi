import streamlit as st
import pandas as pd
import numpy as np
from fetch_data import fetch_data_mykg, fetch_data_mykg_i, fetch_data_id, fetch_data_discovery, fetch_data_capture, fetch_data_offplatform, fetch_data_sap, fetch_data_mykgo, fetch_data_self_input, fetch_data_clel
from datetime import datetime

@st.cache_data(ttl=86400)
def fetch_combined_data():
    # Combine data from multiple sources in a single function
    df_mykg = fetch_data_mykg()
    df_self = fetch_data_self_input()
    df_mykg_i = fetch_data_mykg_i()
    df_id = fetch_data_id()
    df_discovery = fetch_data_discovery()
    df_capture = fetch_data_capture()
    df_offplatform = fetch_data_offplatform()
    df_mykgo = fetch_data_mykgo()
    df_combined_mysql = pd.concat([df_mykg, df_self, df_mykg_i, df_id, df_discovery, df_capture, df_offplatform, df_mykgo], ignore_index=True)

    df_capture['progress'] = 100
    df_offplatform['progress'] = 100


    # Clean email and nik columns efficiently
    df_combined_mysql['email'] = df_combined_mysql['email'].str.strip().str.lower()
    df_combined_mysql['nik'] = df_combined_mysql['nik'].astype(str).str.replace('.0', '', regex=False).str.zfill(6)
    #df_combined_mysql['duration'] = df_combined_mysql['duration'].apply(lambda x: int(float(x)) if pd.notnull(x) else None)
    #df_combined_mysql['duration'] = pd.to_numeric(df_combined_mysql['duration'], errors='coerce').fillna(0).astype(int)

    if 'progress' not in df_combined_mysql.columns:
        df_combined_mysql['progress'] = 100  # Default for Data Capture & Off-Platform
    
    df_combined_mysql.loc[df_combined_mysql['type'] == 'Inclass', 'progress'] = 100
    
    # Convert and filter last_updated column
    if 'last_updated' in df_combined_mysql.columns:
        df_combined_mysql['last_updated'] = pd.to_datetime(df_combined_mysql['last_updated'], errors='coerce').dt.date
        df_combined_mysql = df_combined_mysql.dropna(subset=['last_updated'])
   
    # Override duration based on title
    custom_durations = {
        "Mindfulness Session: Bebaskan Kreativitasmu dengan Mindfulness": 10800,
        "Lelah Mental, Emang Kenapa? [Online Mindfulness Session]": 10800,
        "Memaknai Ragam Peran Perempuan [Online Mindfulness Session]": 10800,
        "Memeluk Anak dalam Diri (Inner Child) [Offline Mindfulness Session]": 10800,
        "Memeluk Anak dalam Diri (Inner Child) [Online Mindfulness Session]": 10800,
        "Memendam vs Melampiaskan Dendam (Kuota Offline Mindfulness Practice)": 10800,
        "Memendam vs Melampiaskan Dendam (Kuota Online Mindfulness Practice)": 10800,
        "Memupuk Harapan dengan Rasa Syukur [Mindfulness Session Kuota Online]": 10800,
        "Memupuk Harapan dengan Rasa Syukur [Mindfulness Session Offline]": 10800,
        "Mengobati Luka dalam Duka [Offline Mindfulness Session]": 10800,
        "Mengobati Luka dalam Duka [Online Mindfulness Session]": 10800,
        "Menjajaki Makna dalam Setiap Langkah Baru [Online Mindfulness Session]": 10800,
        "Lelah Mental, Emang Kenapa? [Offline Mindfulness Session]": 10800,
        "Perlindungan Data Pribadi Dalam Bisnis: Identifikasi, Evaluasi, & Mitigasi": 27000,
        "Sandwich Generation, Hadapi Berbagai Peran [ONLINE Mindfulness Session]": 10800,
        "Satu Tahun yang Tidak Berjalan Begitu Saja [Offline Mindfulness Session]": 10800,
        "Satu Tahun yang Tidak Berjalan Begitu Saja [Online Mindfulness Session]": 10800,
        "Segala yang Tertunda Karena Rasa Malas, Akankah Menunggu? [Offline Mindfulness Session]": 10800,
        "Segala yang Tertunda Karena Rasa Malas, Akankah Menunggu? [Online Mindfulness Session]": 10800,
        "Takut Tambah Dewasa... [Offline Mindfulness Session]": 10800,
        "Takut Tambah Dewasa... [Online Mindfulness Session]": 10800,
        "Terhubung dengan Mereka yang Membentuk Diriku [Offline Mindfulness Session]": 10800,
        "Terhubung dengan Mereka yang Membentuk Diriku [Online Mindfulness Session]": 10800,
        "The Strategic Leader: Building Sustainable and Adaptive Business Strategies": 21600,
        "Bisakah Kita Bersyukur Tanpa Merasa Kurang? [ONLINE Mindfulness Session]": 10800,
        "[Hybrid] Eksplorasi Tanpa Batas! Tenteram Mindfulness Session": 10800,
        "[Online] Menikmati Sendiri tanpa Merasa Sepi (Mindfulness Session)": 10800,
        "Banyak Mau, Emang Butuh? [Offline Mindfulness Session]": 10800,
        "Banyak Mau, Emang Butuh? [Online Mindfulness Session]": 10800,
        "Beda Generasi, Memang Harus Sama? [OFFLINE Mindfulness Session]": 10800,
        "Beda Generasi, Memang Harus Sama? [ONLINE Mindfulness Session]": 10800,
        "Bergerak untuk Berselaras dengan Tubuh, Pikiran, dan Jiwa [Sadar Bergerak & Zumba]": 14400,
        "Berlatih Menemukan Makna pada Keseharian [Mindfulness Session by Tenteram]": 10800,
        "Bertahan di Lingkungan Kerja yang Tak Lagi Nyaman [Offline Mindfulness Session]": 10800,
        "Bertahan di Lingkungan Kerja yang Tak Lagi Nyaman [Online Mindfulness Session]": 10800,
        "Bisakah Kita Bersyukur Tanpa Merasa Kurang? [OFFLINE Mindfulness Session]": 10800,
        "[Hybrid] Berlatih Mengubah Fomo menjadi Jomo! Tenteram Mindfulness Session": 10800,
        "Emang Bisa Kita Berhenti Marah-Marah? (Kuota Online Mindfulness Practice)": 10800,
        "Inner Child: Membebaskan Anak Kecil dalam Rupa Dewasa [Online Mindfulness Session]": 10800,
        "Jika Kamu Ragu untuk Memulai... [Offline Mindfulness Session]": 10800,
        "Jika Kamu Ragu untuk Memulai... [Online Mindfulness Session]": 10800,
        "Jika Kita Berhenti Meragukan Diri [Offline Mindfulness Session]": 10800,
        "Jika Kita Berhenti Meragukan Diri [Online Mindfulness Session]": 10800,
        "Kapan Kita Bisa Merasa Cukup? [OFFLINE Mindfulness Session]": 10800,
        "Kapan Kita Bisa Merasa Cukup? [ONLINE Mindfulness Session]": 10800,
        "Kenapa Jadi Konsisten Itu Sulit? [Mindfulness Session Offline]": 10800,
        "Kenapa Jadi Konsisten Itu Sulit? [Mindfulness Session Online]": 10800
    }
    # Step 1: Clean up duration first
    df_combined_mysql['duration'] = pd.to_numeric(df_combined_mysql['duration'], errors='coerce')

    # Step 2: Overwrite durations from title if matched
    df_combined_mysql['duration'] = df_combined_mysql.apply(
        lambda row: custom_durations[row['title'].strip()]
        if pd.notna(row['title']) and row['title'].strip() in custom_durations
        else row['duration'],
        axis=1
    )

    # Step 3: Final conversion to int (after custom overwrite)
    df_combined_mysql['duration'] = df_combined_mysql['duration'].fillna(0).astype(int)

    # Daftar judul yang bertema Sustainability
    sustainability_titles = [
        "Introduction to Key Concepts - ESG and ESG assessment | Dr Indra Refipal Sembiring, SE, MM",
        "Decoding the “E” of ESG for Environmental Protection and Rehabilitation | Prof. Dr. Ir. Hefni Effendi, M. Phil",
        "Interpreting the “S” of ESG for Social Engagement community and workforce | Dr. Alfian Helmi",
        "Governance: Creating Value through Good Governance Practices | Riskha Dwi Puspitasari",
        "Materiality | Rezky Khairun Zain",
        "ESG Evaluation and Reporting | Dr. Indra Refipal Sembiring, SE, MM",
        "ESG and Sustainable Finance | Fakhrul Aufa, M.M",
        "Best practices : Case Studies and Success Stories | Dr. Iqbal Irfany, SE., M.Sc",
        "Visi dan Inisiatif Lestari KG Media | Andy Budiman",
        "Darurat Krisis Iklim",
        "Inspirasi Perempuan untuk Perubahan Lingkungan:(AMDAL) Anak Muda dan Lingkungan - Ranitya Nurlita",
        "Inspirasi Perempuan untuk Perubahan Lingkungan: Zero Waste Indonesia - Maurillia Imron",
        "Inspirasi Perempuan untuk Perubahan Lingkungan: Dibuang Sayang : Ironi Sampah Makanan - Eva Bachtiar",
        "Inspirasi Perempuan untuk Perubahan Lingkungan:Jalan-Jalan Minim Sampah - Siska Nirmala",
        "Inspirasi Perempuan untuk Perubahan Lingkungan:What Difference One Household Can Make - Siska Nirmala",
        "Podcast - Sustainable Collaboration: Teach People to Listen, not Talk - Ivandeva Wing & Siska Marsudhy",
        "Podcast - Sustainable Collaboration: Train People to Practice Empathy - Ivandeva Wing & Siska Marsudhy",
        "Sustainability Collaboration: Train People to Practice Empathy | Ivandeva Wing & Siska Marsudhy",
        "Podcast - Sustainable Collaboration: Make People More Comfortable with Feedback - Viola Oyong",
        "Jatuh Cinta dengan Sustainability | Wisnu Nugroho",
        "Understanding Sustainability: Memandang Sustainability dalam Bisnis Media | Andreas Maryoto",
        "Understanding Sustainability: Mengapa Kita Harus Peduli? | Didi Kaspi Kasim",
        "Intro to Sustainability"
    ]
    
    # Menambahkan kolom 'category'
    df_combined_mysql['category'] = df_combined_mysql['title'].apply(lambda x: 'Sustainability' if x in sustainability_titles else '')

    return df_combined_mysql

def clean_sap_data(df_sap):
    df_sap['email'] = df_sap['email'].str.strip().str.lower()
    df_sap['nik'] = df_sap['nik'].astype(str).str.zfill(6)
    df_sap['join_date'] = pd.to_datetime(df_sap['join_date'], errors="coerce").dt.date
    return df_sap

def lookup_nik(df_combined_mysql, df_sap):
    # Create email-to-nik mapping dictionary
    email_to_nik = dict(zip(df_sap['email'], df_sap['nik']))

    # Use vectorized operations for the lookup
    nik_match = df_combined_mysql['nik'].isin(df_sap['nik'])
    email_match = df_combined_mysql['email'].map(email_to_nik).notna()
    df_combined_mysql.loc[nik_match, 'count AL'] = df_combined_mysql.loc[nik_match, 'nik']
    df_combined_mysql.loc[~nik_match & email_match, 'count AL'] = df_combined_mysql.loc[~nik_match & email_match, 'email'].map(email_to_nik)
    df_combined_mysql.loc[~nik_match & ~email_match, 'count AL'] = df_combined_mysql.loc[~nik_match & ~email_match, 'email']

    return df_combined_mysql['count AL']

@st.cache_data(ttl=86400)
def finalize_data():
    # Fetch combined data from MySQL sources
    df_combined_mysql = fetch_combined_data()

    # Fetch SAP data with selected columns
    selected_columns = ['name_sap', 'email', 'nik', 'unit', 'subunit', 'layer', 'generation', 'gender', 'division', 'department', 'region', 'admin_goman', 'penugasan', 'join_date']
    df_sap = fetch_data_sap(selected_columns)
    df_sap = clean_sap_data(df_sap)

    # Apply lookup function to each row
    df_combined_mysql['count AL'] = lookup_nik(df_combined_mysql, df_sap).astype(str)
    
    # Merge combined MySQL data with SAP data
    merged_df = pd.merge(df_combined_mysql, df_sap, left_on='count AL', right_on='nik', how='left', indicator=True)
    merged_df['status'] = merged_df['_merge'].apply(lambda x: 'Internal' if x == 'both' else 'External')
    merged_df.drop(columns=['_merge'], inplace=True)
    #merged_df.drop(columns=[''], inplace=True)

    # Convert specific columns to string
    columns_to_convert = ['email_x', 'name', 'unit', 'subunit', 'division']
    merged_df[columns_to_convert] = merged_df[columns_to_convert].astype(str)

    # Right join to include all rows from df_sap
    right_merged_df = pd.merge(df_combined_mysql, df_sap, left_on='count AL', right_on='nik', how='right', indicator=True)
    right_merged_df['status'] = right_merged_df['_merge'].apply(lambda x: 'Active' if x == 'both' else 'Passive')
    right_merged_df.drop(columns=['_merge'], inplace=True)

    safe_string_columns = ['layer', 'division', 'unit', 'subunit', 'generation', 'region', 'department']
    for col in safe_string_columns:
        if col in df_combined_mysql.columns:
            df_combined_mysql[col] = df_combined_mysql[col].fillna('').astype(str)




    return merged_df, df_combined_mysql, df_sap, right_merged_df

@st.cache_data(ttl=86400)
def finalize_data_clel():
    df_clel = fetch_data_clel()
    if 'last_updated' in df_clel.columns:

        df_clel['last_updated'] = pd.to_datetime(
            df_clel['last_updated'],
            errors='coerce',
            dayfirst=False,
        ).dt.date
        
    else:
        pass

    return df_clel

