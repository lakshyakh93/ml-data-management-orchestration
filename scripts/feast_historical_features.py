import feast
import pandas as pd

def get_feast_historical_features():

    fs = feast.FeatureStore(repo_path="/home/ubuntu/data-management-assignment/telco_churn_feast_repo/feature_repo")

    entity_df = pd.DataFrame.from_dict({"customerID": ["7590-VHVEG", "5575-GNVDE", "3668-QPYBK", "7795-CFOCW", "9237-HQITU", "9305-CDSKC", "1452-KIOVK", "6713-OKOMC", 
    "7892-POOKP", "6388-TABGU", "9763-GRSKD", "7469-LKBCI", "8091-TTVAX", "0280-XJGEX", "5129-JLPIS", "3655-SNQYZ", "8191-XWSZG", "9959-WOFKT", "4190-MFLUW",
    "4183-MYFRB", "8779-QRDMV", "1680-VDCWW", "1066-JKSGK", "3638-WEABW", "6322-HRPFA", "6865-JZNKO", "6467-CHFZW", "8665-UTDHZ", "5248-YGIJN", "8773-HHUOZ", 
    "3841-NFECX", "4929-XIHVW", "6827-IEAUQ", "7310-EGVHZ", "3413-BMNZE", "6234-RAAPL", "6047-YHPVI", "6572-ADKRS", "5380-WJKOV", "8168-UQWWF", "8865-TNMNX", 
    "9489-DEDVP", "9867-JCZSP", "4671-VJLCL", "4080-IIARD", "3714-NTNFO", "5948-UJZLF", "7760-OYPDY", "7639-LIAYI", "2954-PIBKO"]})
    
    # Adding arbitrary timestamps for the data
    timestamps = pd.date_range(
        end=pd.Timestamp.now(), 
        periods=len(entity_df), 
        freq='D').to_frame(name="event_timestamp", index=False)

    entity_df = pd.concat(objs=[entity_df, timestamps], axis=1)

    training_df = fs.get_historical_features(
        entity_df=entity_df,
        features = [
            "telco_customer_stats:gender",
            "telco_customer_stats:SeniorCitizen",
            "telco_customer_stats:Partner",
            "telco_customer_stats:Dependents",
            "telco_customer_stats:tenure",
            "telco_customer_stats:PhoneService",
            "telco_customer_stats:MultipleLines",
            "telco_customer_stats:InternetService",
            "telco_customer_stats:OnlineSecurity",
            "telco_customer_stats:OnlineBackup",
            "telco_customer_stats:DeviceProtection",
            "telco_customer_stats:TechSupport",
            "telco_customer_stats:StreamingTV",
            "telco_customer_stats:StreamingMovies",
            "telco_customer_stats:Contract",
            "telco_customer_stats:PaperlessBilling",
            "telco_customer_stats:PaymentMethod",
            "telco_customer_stats:MonthlyCharges",
            "telco_customer_stats:TotalCharges",
            "telco_customer_stats:TotalAddonServices",
            "telco_customer_stats:AvgMonthlyUsage",
            "telco_customer_stats:TenureGroup",
            "telco_customer_stats:Churn",
        ],
    ).to_df()

    print(training_df)


get_feast_historical_features()
