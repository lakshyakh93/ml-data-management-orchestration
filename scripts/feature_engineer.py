def add_new_features(RAW_DATA_LOCAL_PATH, TRANSFORMED_DATA_LOCAL_PATH):

    import pandas as pd
    from tqdm import tqdm

    def get_new_features_for_row(row):

        if len(row["TotalCharges"].strip()) == 0:
            return None

        # New Feature TotalAddonServices
        addon_service_cols = ['OnlineSecurity', 'OnlineBackup', 'DeviceProtection', 'TechSupport', 'StreamingTV', 'StreamingMovies']

        addon_service_bools = [True if row[col].lower() == "yes" else False for col in addon_service_cols]
        row["TotalAddonServices"] = int(addon_service_bools.count(True))

        # New Feature AvgMonthyUsage
        row["AvgMonthlyUsage"] = round(float(row["TotalCharges"])/row["tenure"], 2)

        # Apply the function to create the 'TenureGroup' feature
        row['TenureGroup'] = 'Short-Term' if row['tenure'] < 12 else ('Medium-Term' if row['tenure'] < 36 else 'Long-Term')

        return row

    tqdm.pandas()
    df = pd.read_csv(RAW_DATA_LOCAL_PATH)
    df = df.progress_apply(get_new_features_for_row, axis=1)
    df.to_csv(TRANSFORMED_DATA_LOCAL_PATH)

