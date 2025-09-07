import pandas as pd

def versionizer(dataframe):
    clean_df = dataframe.dropna(subset=['update_dt']).copy()
    df_upd = dataframe[dataframe['update_dt'].isna()].copy()
    if not df_upd.empty:
        df_upd.loc[:, 'version'] = df_upd['order_dt'].apply(lambda x: int(x.timestamp()) if pd.notna(x) else 0).astype('int64')
    clean_df.loc[:, 'version'] = clean_df['update_dt'].apply(lambda x: int(x.timestamp()) if pd.notna(x) else 0).astype('int64')
    return pd.concat([clean_df, df_upd], ignore_index=True)