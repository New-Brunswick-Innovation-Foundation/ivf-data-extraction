numeric_columns = ['FedLeverage', 'OtherLeverage', 'FTE', 'PTE']

sector_mapping = {
    "Environment & Agriculture - Select Sector": [
        "Environmental Technology & Resource Management",
        "Fisheries & Marine Sciences",
        "Agriculture, Forestry, Food & Beverage"
    ],
    "Information Technology - Select Sector": [
        "ICT",
        "Energy & Electronics",
        "Manufacturing & Materials",
        "Aerospace & Defense",
        "Precision Sciences"
    ],
    "BioScience and Health - Select Sector": [
        "Bioscience & Biotechnology",
        "Health & Medicine"
    ],
    "Business Operations - Select Sector": [
        "Statistics & Data Analytics",
        "Finance, Economics & Business Sciences",
        "Consumer Goods & Services",
        "Media, Tourism & Entertainment"
    ],
    "Social Sciences - Select Sector": [
        "Social Sciences & Humanities, Psychology"
    ]
}

city_to_region_mapping = {
    "Fredericton": "SW",
    "Moncton": "SE",
    "Saint John": "SW",
    "Bathurst": "NE",
    "Campbellton": "NE",
    "Miramichi": "NE",
    "Edmundston": "NW",
    "Caraquet": "NE",
    "Shippagan": "NE",
    "Dieppe": "SE",
    "Riverview": "SE",
    "Tracadie-Sheila": "NE",
    "Sackville": "SE",
    "St. Stephen": "SW",
    "Sussex": "SE",
    "Quispamsis": "SW",
    "Rothesay": "SW",
    "Hanwell": "SW",
    "Clair": "NW",
    "St. Andrews": "SW",
}

province_mapping = {
    0: "AB",
    1: "BC",
    2: "MB",
    3: "NB",
    4: "NL",
    5: "NT",
    6: "NS",
    7: "NU",
    8: "ON",
    9: "PE",
    10: "QC",
    11: "SK",
    12: "YK"
}

TABLE_CONFIGS = {
    'Investment': {
        'unique_column': 'RefNum',
        'filter_column': 'ResearchFundID',
        'columns': [
            'RefNum', 'ApplTitle', 'ExecSum', 'FiscalYear', 'ResearchFundID',
            'ApplDate', 'DecisionDate', 'AmtRqstd', 'AmtAwarded', 'TotalLevAmt',
            'PrivSectorLev', 'FedLeverage', 'OtherLeverage', 'FTE', 'PTE',
            'NBIFSectorID', 'Notes'
        ]
    },
    'VoucherCompany': {
        'unique_column': 'CompanyName',
        'filter_column': None,
        'columns': [
            'CompanyName', 'Address', 'City', 'Province',
            'PostalCode', 'Country', 'Region', 'IncorporationDate'
        ]
    }
    ,
    'PeopleInfo': {
        'unique_column': 'Email',
        'filter_column': None,
        'columns': [
            'LastName', 'FirstName', 'Email', 'Phone', 
            'Note', 'CommOptOut'
        ]
    }
}

years = [
    "2026",
    "2025",
    "2024",
    "2023-2024",
    "2022-2023",
    "2021-2022",
    "2020-2021",
    "2019-2020",
    "2018-2019",
    "2017-2018",
    "2016-2017",
    "2015-2016",
    "2014-2015",
    "2013-2014",
    "2012-2013",
    "2011-2012"
]

regions = [
    "SW",
    "SE",
    "NW",
    "NE"
]

banner = """
    ██████╗ ██████╗  ██╗     
    ██╔══██╗██╔═══██╗██║     
    ██████╔╝██║   ██║██║     
    ██╔═══╝ ██║   ██║██║     
    ██║     ╚██████╔╝███████╗
    ╚═╝      ╚═════╝ ╚══════╝

    Portal Database Linkage (PDL)
  Connecting SMApply → SQL for historical, 
    accurate, and automated storage.
---------------------------------------------------
"""