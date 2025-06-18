
from langchain.chat_models import ChatOpenAI
from langchain import LLMChain
from langchain.prompts import PromptTemplate
from rapidfuzz import process,fuzz
from collections import defaultdict
import re

entities_by_category = {
    "ProductHeirachy1": [
        "milk", "curd","Bread & Bakery", "Butter", "ButterMilk", "Cheese", "Cold Coffee", "Cream", "Curd", "Dairy Whitener", "Doodh Peda","Flav.Milk", "Frozen Dessert", "Ghee", "Gluco Shakti", "Gulab Jamun", "Hot Drinks", "IceCream", "Immunity Milk", "Khoa", "Laddu", "Lassi", "Medicine", "Milk", "Milk Cake", "Milk Shakes", "Nestle Dahi", "Nestle Misthi Doi", "Nestle Raita", "Nestle Yoghurt", "Others", "Paneer", "Ragi Java", "Rasgulla", "Savories", "Shrikhand", "SkimMilk Powder", "Soan Papdi", "Veterinary Medicines", "Water", "Whey Drink", "Whole Milk Powder"
    ],
"ProductHeirachy2": [
    "Buffalo", "Cow", "Default", "E&I", "MASH", "Mixed", "PELLET"
],
"ProductHeirachy3": [
    "Afghan Delight", "Agmarked", "Almond Crunch", "American Delight", "Amrakhand", "Anjeer Badam", "Anjeer Kulfi", "Ashwagandha", "B Grade", "Badam", "Badam Nuts", "Badam Pista Kesar", "Banana Cinnamon", "Banana Strawberry", "Belgium Chocolate", "Berry Blast", "Berry Burst", "Besan", "BFCM", "Black Currant Vanilla", "Black Current", "Blocks", "Blueberry Yoghurt", "Bubble Gum", "Butter Scotch", "Butterscotch Bliss", "Butterscotch Crunch", "BYPASS 6MM", "CAL", "Caramel Nuts", "Caramel Ripple Sundae", "Cassatta", "Choco chips", "Choco Rock", "Chocobar", "Chocolate", "Chocolate Coffee Fudge", "Chocolate Overload", "Classic Kulfi", "Classic Vanilla", "Coffee", "Cold Coffee", "Cookies & Cream", "Cookies-n-Cream", "Cotton Candy", "Cubes", "Cultured", "Default", "DELUXE", "DELUXE 6MM", "Double Chocolate", "DTM", "Elachi", "Evaporated Skimmilk", "FCM", "Fig Honey", "Fresh 'N' Natural", "Fresh 'N' Natural Slim", "Fresh Strawberry", "Fruit Fantasy", "Fruit Fusion", "Ginger", "Gol Gappa", "GOLD", "GOLD 6MM", "Golden Cow Milk", "Grape Jelly", "Grape Juicy", "Gulkhand Kulfi", "Herbs&Garlic", "HONEY NUTS", "Hot Milk", "ISI", "Jeera", "Jowar", "Kala Khatta", "Kohinoor Kulfi", "Laddoo Prasadam", "LATTE", "Lemon", "LIV", "Low Fat", "Malai Kulfi", "Mango", "Mango Alphanso", "Mango Juicy", "Mango Lychee", "Mango Masti Jusy", "Mango Tango", "Mango Yoghurt", "Mawa Kulfi", "Mega-Sundae", "Melon Rush", "MILK GOLD 4MM", "MIN", "MISHTI", "Mixed Berry Sundae", "Mixed Millet", "MOCHA", "Mocha Crunch", "Mozarella", "Nesvita", "Non Agmarked", "NonAgmarked", "Orange", "Orange Juicy", "Orange Tango", "Pan Kulfi", "Pasterised", "Pine Apple", "Pineapple", "Pineapple Passsion", "Pink Guava", "Pista", "Pistachio", "Plain", "Pot Kulfi (Pista)", "POWER 4MM", "Premium Vannila", "Probiotic", "Probiotic TM", "Rajbhog", "Rasperry Twin", "Raw", "Raw Chilled", "Real Blue Berry", "Roasted Cashew", "Rose", "Royal Rose Delight", "Sabja", "Salted", "Salted Caramel", "Shrikhand Kesar", "Sitaphal", "Skim Milk", "Slices", "Slim", "Special", "SPECIAL 6MM", "Spreads", "Standard Coffee", "Standard TEA", "STANDY", "STD", "STD Milk", "STD MLK Curd", "Strawberry", "Strawberry Yoghurt", "Strawbery", "SUPREME 6MM", "Sweet", "Tasty Kulfi", "Tikka", "TM", "Tulsi", "Turmeric", "Twin Vanilla&Strawberry", "Vanilla", "Vanilla&Strawberry", "VIT"
],
"ProductHeirachy4": [
    "Alluminium Foil  Pack", "Alu. Foil Pack", "Alu.Foil", "Aluminium Foil  Pack", "Ball", "Ball (Ice Cream)", "Box", "Bucket", "Bulk", "Can", "Carton", "Ceka Pack", "Cheeka Pack", "Cone", "Cup", "Disp.Bottle", "Glass Bottle", "GUNNY", "HDPE Bag", "Jar", "Matka", "Pillow Pack", "Poly Pack", "PolyPack", "Pouch", "PP", "PP + Box", "PP Bottle", "Sachets", "Spout Pouch", "STANDY POUCH", "Stick", "Stick (Ice Cream)", "Tetra Pack", "Tin", "Tray", "Tub", "UHT Poly Pack"
],
"ProductHeirachy5": [
    "1 KG", "1 Litre", "1 Litres", "10 KG", "10 Litres", "100 GMS", "100 ML", "1000 GMS", "1000 ML", "100ML", "110 GMS", "110 ML", "110ML", "115 GMS", "120 GMS", "120 ML", "125 GMS", "125 ML", "125ML", "12ML", "130 GMS", "130 ML", "135 GMS", "135 ML", "140 GMS", "140 ML", "145 GMS", "145 ML", "15 KG", "150 GMS", "150 ML", "155 GMS", "155 ML", "16 GMS", "160 GMS", "160 ML", "165 GMS", "165 ML", "17 GMS", "170 GMS", "170 ML", "175 GMS", "175 ML", "18.2 KG", "180 GMS", "180 ML", "182 GMS", "185 GMS", "185 ML", "190 ML", "2 KG", "2 Litres", "2.5 KG", "20 GMS", "20 KG", "20 Litres", "200 GMS", "200 ML", "220 GMS", "220 ML", "225 GMS", "225 ML", "230 ML", "240 GMS", "240 ML", "25 KG", "250 GMS", "250 ML", "25ML", "273 GMS", "275 GMS", "28 GMS", "280 ML", "30 ML", "300 GMS", "300 ML", "300GMS", "310 ML", "325 ML", "33 GMS", "330 ML", "35 GMS", "35 ML", "350 GMS", "350 ML", "35ML", "360 GMS", "375 GMS", "375 ML", "380 GMS", "4 liters", "4 Litres", "4.5 KG", "4.70 KG", "40 KG", "40 Litres", "40 ML", "400 GMS", "400 ML", "400GMS", "40ML", "425 GMS", "425 ML", "425+50 GMS", "440 ML", "450 GMS", "450 ML", "450+50 GMS", "475 GMS", "475 ML", "475+50 GMS", "480 GMS", "480 ML", "485 ML", "490 ML", "5 Kg", "5 Liter", "5 Litres", "50 GMS", "50 KG", "50 ML", "500 GMS", "500 ML", "500+50 GMS", "6 Liter", "60 GMS", "60 KG", "60 ML", "60ML", "63 GMS", "65 ML", "70 GMS", "70 ML", "700 ML", "700+700ML", "700ML", "75 GMS", "750 ML", "80 GMS", "80 ML", "800 ML", "85 GMS", "850 GMS", "9 KG", "9.1 KG", "90 GMS", "90 ML", "900 GMS", "900 ML", "90ML", "950 GMS", "950 ML", "975 ML", "990 ML"
],
"Materialgroup": [
     "AMRAKHAND", "BESAN LADDU", "Bucket Curd", "BUTTER MILK", "Butter Milk (cup)", "BUTTER MILK (SIG)", "CHEESE", "COLD COFFEE", "COLD COFFEE (SIG)", "Cooking Butter - Bulk", "Cooking Butter - CP", "Cream Bulk", "Cup Curd", "Doodhpeda", "FLAVOURED MILK (GB)", "Flavoured Milk (Glass)", "FLAVOURED MILK (PP)", "Flavoured Milk (TP)", "FMCG/Bakery", "FRESH CREAM-CP", "Fruit Lassi (Cup)", "FRUIT LASSI (PP BOTTLE)", "FRUIT LASSI (SIG)", "Ghee Bulk", "Ghee CP", "GULAB JAMUN", "HOT DRINKS", "Ice Cream/FD", "IMMUNITY MILK", "JOWAR LADDU", "Milk Cake", "MILK SHAKES", "MIXED MILLET LADDU", "NESTLE", "Paneer", "Pouch Curd", "RAGI JAVA (CUP)", "RASGULLA", "SHRIKHAND KESAR", "SMP BULK", "SMP CP", "Soan Papdi", "SPICED BUTTER - CP", "Sweet Lassi (Cup)", "Sweet Lassi (Pouch)", "SWEET LASSI (PP BOTTLE)", "SWEET LASSI (SIG)", "Table Butter - CP", "UHT Milk", "WHEY DRINKS", "WHEY DRINKS (SIG)", "WHEY DRINKS CUP"
],
"CITY": [
    "BANGALORE",
    "BHUBANESWAR",
    "BIDAR",
    "BOBBILI",
    "CHENNAI",
    "CHITTOOR",
    "COCHIN",
    "COIMBATTORE",
    "Cuttack",
    "DELHI NCR",
    "ERODE",
    "GHAZIABAD",
    "Gollapudi",
    "Guntur",
    "HARYANA",
    "HUBLI",
    "HYDERABAD",
    "Indore",
    "JAIPUR",
    "KALLURU",
    "Kolkatta",
    "LUCKNOW",
    "Mangalagiri",
    "MORENA",
    "MUMBAI",
    "Nagpur",
    "Nellore",
    "ORISSA",
    "PALGHAR",
    "Patna",
    "Pune",
    "PUNJAB",
    "Raipur",
    "RAJAHMUNDRY",
    "SANGVI",
    "SINDHANUR",
    "TIRUPATI",
    "UP & UK",
    "Uttarakhand",
    "VADAMADURAI",
    "VIJAYAWADA",
    "VISAKHAPATNAM",
    "WESTBENGAL"
],
"STATES": [
    "ANDHRA PRADESH",
    "BIHAR",
    "CHHATTISGARH",
    "DELHI",
    "HARYANA",
    "KARNATAKA",
    "KERALA",
    "MADHYA PRADESH",
    "MAHARASHTRA",
    "ODISHA",
    "PUNJAB",
    "RAJASTHAN",
    "TAMIL NADU",
    "TELANGANA",
    "UTTAR PRADESH",
    "UTTARANCHAL",
    "WEST BENGAL"
],
"SHORT_NAME": [ "BBLSO", "BBLSO 6", "BHANDARA SO", "BHUBANESHWER SO", "BIDAR SO", "BKKSO", "Brahmpur - So", "BSO 1", "BSO 2", "BSO 3", "BSO 4", "BSO 5", "BSO 6", "BSO 8", "Cochin", "Cochin sales office-so", "COIMBATTORE SO", "CSO 1", "CSO 2", "CSO 3", "CSO 4", "CSO 5", "CSO 6", "CSO 8", "CTRSO", "CTRSO 6", "CUTTACK", "DINDIGUL SO", "DSO", "DSO 6", "ERODE SO", "EXPORT SALES - KA", "EXPORT SALES - TN", "EXPORT SALES OFFICE - AP", "EXPORT SALES OFFICE - MH", "EXPORT SALES OFFICE - TS", "Ghazizbad SO", "GLPSO", "GNTSO", "GSO", "GSO 2(ATPSO)", "GSO 6", "GSO 8", "HSO 1", "HSO 10", "HSO 11", "HSO 12", "HSO 13", "HSO 14", "HSO 15", "HSO 17(KA)", "HSO 2", "HSO 3", "HSO 4", "HSO 5", "HSO 6", "HSO 7", "HSO 8", "HSO 8 UP CTRY", "HSO 9", "HUBLI SO", "INDORE", "JAIPUR SO", "KALADERA SO", "KANCHIPURAM SO", "KKDSO", "KLKSO", "KLRSO", "KMNO SO", "Kolkatta Sales Office", "KUNDLI SO", "LUCKNOW SO", "LUDHIANA SO", "MGLSO", "MNSO", "MORENA SO", "MPSO", "MSO", "MSO 2", "MSO 6", "MSO 7", "MSO 8", "NAGPUR", "NELLORE SO", "NELLORE SO2", "NKPSO", "ONGOLE SO", "ORISSA", "PATNA", "PMSO", "PMSO 6", "PMSO 8", "PSO", "PUNE", "PUNE SO1", "RAI SO", "RAI SO 1", "RAI SO 2", "RAI SO 3", "RAI SO 4", "RAI SO 6", "RAIPUR", "RSO", "SALEM SO", "SDNSO", "SMPTS", "SNVSO", "SNVSO trade", "THANE SO", "TNKSO", "TSO", "Uttarakhand SO", "VASAI SO", "VELLORE SO", "VJASO", "VJASO 6", "VJASO 8", "VMISO", "VMISO-6", "VSO", "VSO 2", "VSO 6", "VSO 8"
],
"REGION_NAME": ["AP-1", "AP-2", "AP-3", "EAST", "KA", "MH", "MP", "OD", "TG", "TG-1", "TG-2", "TN", "UP", "Z-4"],
"CustomerGroup": [
    "Agents", "B to C", "Bulk Sales", "Conversion", "Direct Consumer Sale", "Distributor", "E & I Customers", "E-Commerce", "Employees", "Fresh Distributor", "HDC", "Institutions", "Modern Formats", "MRF Distributor", "OMO Distributor", "Others", "Parlours", "Plant Parlours", "Push-Cart Distributr", "Stockiest", "Stockiest / Distrib.", "Super Stockiest", "TCD Retailers"
],
"DistributionChannel": ["Direct", "Parlours"], 
"PLANT_NAME": [
    "B.KOTHAKOTA SALES OFFICE",
    "Bangalore Sales Office - 1",
    "Bangalore Sales Office - 2",
    "Bangalore Sales Office - 3",
    "Bangalore Sales Office - 4",
    "Bangalore Sales Office - 5",
    "Bangalore Sales Office - 6",
    "Bangalore Sales Office - 8",
    "Battiprolu Sales office",
    "Bhandara Sales Office",
    "Bhubaneswar Sales Office",
    "BIDAR",
    "Bobbili Sales Office",
    "BOBBILI SALES OFFICE-6",
    "Chennai Sales Office - 1",
    "Chennai Sales Office - 2",
    "Chennai Sales Office - 3",
    "Chennai Sales Office - 4",
    "Chennai Sales Office - 5",
    "Chennai Sales Office - 6",
    "Chennai Sales Office - 8",
    "Chittoor Sales Office",
    "CHITTOOR SALES OFFICE-6",
    "Cochin Sales Office (C&F)",
    "Cochin sales office-so",
    "COIMBATTORE SALES OFFICE",
    "Cuttack Sales Office (C&F)",
    "Delhi Sales Office",
    "Delhi Sales Office - 6",
    "Dindigul Sales Office",
    "ERODE",
    "Export Sales Office - AP",
    "Export Sales Office - KA",
    "Export Sales Office - MH",
    "Export Sales Office - TN",
    "Export Sales Office - TS",
    "Ghaziabad Sales Office",
    "Gokul Sales Office",
    "GOKUL SALES OFFICE-6",
    "Gokul Sales Office-8",
    "Gokul SO - 2 (ATPSO)",
    "Gollapudi Sales Office",
    "Guntur Sales Office",
    "Hubli SO",
    "Hyd Sales Office - 17 (KA)",
    "Hyderabad Sales Office - 1",
    "Hyderabad Sales Office - 10",
    "Hyderabad Sales Office - 11",
    "Hyderabad Sales Office - 12",
    "Hyderabad Sales Office - 13",
    "HYDERABAD SALES OFFICE - 14 - Catg-2",
    "Hyderabad Sales Office - 15",
    "Hyderabad Sales Office - 2",
    "Hyderabad Sales Office - 3",
    "Hyderabad Sales Office - 4",
    "Hyderabad Sales Office - 5",
    "Hyderabad Sales Office - 6",
    "Hyderabad Sales Office - 7",
    "Hyderabad Sales Office - 8",
    "Hyderabad Sales Office - 9",
    "Hyderabad SO UP Ctry - 8",
    "Indore Sales Office (C&F)",
    "Jaipur Sales Office",
    "Kakinada Sales Office",
    "Kaladera Sales Office",
    "Kalluru Sales Office",
    "Kanchipuram",
    "Khamanon Sales Office",
    "Kolkatta Sales Office",
    "Kundli Sales Office",
    "LUCKNOW",
    "Ludhiana Sales Office",
    "Mangalagiri Sales Office",
    "MANOR SALES OFFICE",
    "Morena Sales Office",
    "MP Sales office",
    "Mumbai Sales Office",
    "Mumbai Sales Office - 2",
    "Mumbai Sales Office - 7",
    "Mumbai Sales Office 6",
    "Mumbai Sales Office-8",
    "Nagpur",
    "Narketpally Sales Office",
    "Nellore Sales office",
    "ONGOLE SO",
    "Orissa Sales Office",
    "Pamarru Sales Office",
    "Pamarru Sales Office-6",
    "Pamarru Sales Office-8",
    "Patna Sales Office (C&F)",
    "Pune Sales Office",
    "Pune Sales Office (C&F)",
    "Pune Sales Office1(Dhankawadi)",
    "Rai Sales Office",
    "Rai Sales Office - 6",
    "Rai Sales Office 1",
    "Rai Sales Office 2",
    "Rai Sales Office 3",
    "Rai Sales Office 4",
    "RAIPUR SO (C&F)",
    "Rajahmundry Sales Office",
    "Salem Sales Office",
    "Sangvi Sales Office",
    "Sangvi Trading Sales Office",
    "SHAMIRPET SALES OFFICE",
    "Sindhanur Sales Office",
    "Tanuku Sales Office",
    "Thane Sales Office",
    "Tirupati Sales Office",
    "Traimbakam- Sales Office",
    "Uttarakhand Sales Office",
    "Vadamadurai Sales Office",
    "Vadamadurai Sales Office 6",
    "Vasai (Thane) Sales Office",
    "Vellore Sales Office",
    "Vijayawada Sales Office",
    "Vijayawada Sales Office-6",
    "Vijayawada Sales Office-8",
    "Vizag Sales Office",
    "VIZAG SALES OFFICE-2",
    "Vizag Sales Office-6",
    "Vizag Sales Office-8"
]

}

from collections import defaultdict

import string
import re
from rapidfuzz import fuzz

stop_words = set([
    'livo','hey','wise',
    'achievement', 'achievements', 'achieved', 'achieve', 'achieving','badget',
    'GT', 'actual', 'target', 'vs', 'budget', 'opm', 'opy', 'nka', 'NKA',
    'lastweek', 'atleast', 'at least', 'last week', 'very',
    'billed', 'unbilled', 'not billed', 'their', 'names',
    'unbilled', 'customers', 'low', 'high', 'top', 'bottom', 'highest', 'lowest',
    'performing', 'min', 'sku', 'performed', 'well', 'state',
    'a', 'an', 'the', 'is', 'are', 'was', 'were', 'be', 'been', 'being',
    'am', 'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would',
    'shall', 'should', 'can', 'could', 'may', 'might', 'must',
    'to', 'from', 'by', 'with', 'about', 'against', 'between', 'into',
    'through', 'during', 'before', 'after', 'above', 'below', 'up', 'down',
    'in', 'out', 'on', 'off', 'over', 'under',
    'and', 'or', 'but', 'if', 'because', 'as', 'until', 'while',
    'of', 'at', 'for', 'so', 'nor', 'yet',
    'this', 'that', 'these', 'those', 'here', 'there',
    'then', 'once', 'again', 'more', 'most', 'less', 'least', 'very',
    'just', 'only', 'own', 'same', 'such',
    'what', 'which', 'who', 'whom', 'whose', 'how', 'when', 'where', 'why',
    'iin', 'nrv', 'ubc',
    'every', 'give', 'me', 'region', 'sale', 'yesterday', 'name',
    'its', 'quantity', 'single', 'token',
    'highest', 'one', 'has', 'come', 'first',
    'product', 'tell', 'lowest', 'top', 'bottom', 'last', 'next', 'previous',
    'value', 'added', 'products', 'market', 'salesoffice', 'salesoffices',
    'salesofficeid', 'salesofficename', 'salesofficetype', 'salesofficetypeid',
    'you', 'high', 'year ', 'compare', 'growth',
    
    # Added common basic English words frequent in Indian English
    'i', 'he', 'she', 'we', 'they', 'it',
    'please', 'thanks', 'sorry', 'sir', 'madam',
    'time', 'person', 'year', 'way', 'day', 'thing', 'man', 'woman', 'child',
    'school', 'work', 'family', 'friend', 'place', 'food', 'water', 'money',
    'good', 'new', 'long', 'great', 'little', 'other', 'old', 'right', 'big',
    'high', 'different', 'small', 'large',
    'can', 'will', 'should', 'might', 'would', 'could',
    'very', 'really', 'always', 'often', 'sometimes', 'never', 'soon', 'here', 'now', 'then',
    'one', 'two', 'three', 'some', 'any', 'many', 'much', 'few', 'all', 'main', 'april', '2025', 'growth', 'month',
])

# List of short keywords to allow only if clearly mentioned
critical_short_terms = {"tm", "od", "fc"}

# Normalize text for fuzzy matching: remove punctuation, spaces, hyphens, lowercase
def normalize(text):
    return text.lower().translate(str.maketrans('', '', string.punctuation)).replace(" ", "").replace("-", "")

# Tokenize and remove stopwords from text
def tokenize_and_clean(text):
    tokens = re.findall(r'\b\w+\b', text.lower())  # extract words only
    return [t for t in tokens if t not in stop_words]

# Main fuzzy matching function
def fuzzy_match_entities(user_query, entities_by_category, threshold=85):
    words = tokenize_and_clean(user_query)  # remove stopwords first
    filtered_query = " ".join(words)        # reconstruct query without stopwords
    norm_query = normalize(filtered_query)  # normalize after stopword removal
    word_set = set(words)                    # for quick membership checks

    matched = {}

    print("query sent to fuzzy is:", norm_query)

    for column, values in entities_by_category.items():
        for value in values:
            norm_value = normalize(value)
            clean_value = value.lower()

            # Only match short critical terms if clearly present as a token
            if len(clean_value) <= 3 and clean_value in critical_short_terms:
                if clean_value in word_set:
                    matched[value] = (column, 100)
                continue

            # 1) Exact substring match in cleaned, normalized query
            if norm_value in norm_query:
                matched[value] = (column, 100)
                continue

            # 2) Fuzzy match full cleaned query vs entity value
            score = fuzz.token_set_ratio(norm_query, norm_value)
            if score >= threshold:
                matched[value] = (column, score)
                continue

            # 3) Fuzzy match each cleaned token vs entity value
            for word in words:
                norm_word = normalize(word)
                score = fuzz.token_set_ratio(norm_word, norm_value)
                if score >= threshold:
                    matched[value] = (column, score)
                    break  # stop after first good match

    # Return dictionary mapping matched entity values to their columns
    return {value: column for value, (column, _) in matched.items()}

business_term_mapping = {
    "top":"top means use desc",
    "amount":"[amount means you can calculate like this : SUM(NetAmount) FROM DW.fSales]",
    "salesoffice":"[slaesoffice means you have to consider: Short_Name from DW.dSalesOfficeMaster]",
    "salesoffices":"[slaesoffice means you have to consider: Short_Name from DW.dSalesOfficeMaster]",
    "sos":"[slaesoffice means you have to consider: Short_Name from DW.dSalesOfficeMaster]",
"livo": "[livo means strictly you have to map like this: FROM DW.fSales WHERE MaterialCode IN ('30769', '30704', '30706')]",
    "growth":"[growth means: GrowthPercent = ((Current - Previous) / Previous) * 100]",
    "opm":"[opm means Compare this month to lastmonth/previous month]",
    "opy":"[opy means Compare this month this year vs. same month last year]",
    "lastmonth":"[to calculate no.of days in lastmonth use DAY(EOMONTH(DATEADD(MONTH, -1, GETDATE())))]",
    "UBC": "[COUNT(DISTINCT CustomerID) NOT IN ('Feed', 'Feed Supplement', 'Veterinary Medicines',Water','HERITA LIV','NULL','others')]",
    "unique billing count":"[COUNT(DISTINCT CustomerID) NOT IN ('Feed', 'Feed Supplement', 'Veterinary Medicines',Water','HERITA LIV','NULL','others')]",
    "milk DTM":"DTM",
    "hyd":"hyd means HYDERABAD",
    "sale quantity": "[sale quantity means SUM(SalesQuantity) / number_of_days_in_time_period, where number_of_days_in_time_period = total full days in the query date range] and MAX(f.SalesUnit) AS SalesUnit only when user mentioned any product from any ProductHeirachy or material group]",
    "total tax": "SUM(TotalTax)",
    "total amount": "SUM(TotalAmount) NOT IN ('Feed', 'Feed Supplement', 'Veterinary Medicines')",
    "butter milk": "buttermilk",
    "butter Milk": "buttermilk",
    "Butter Milk": "buttermilk",
    "city": "CITY",
    "state": "STATE",
    "mik":"[milk means consider only ProductHeirachy1]",
    "Milk":"[milk means consider only ProductHeirachy1]",
    "REGION NAME": "REGION_NAME",
    "region name": "REGION_NAME",
    "area name": "AREA_NAME",
    "short Name": "Short_Name",
    "short name":"Short_Name",
    "sales Office type": "Sales Offcie Type",
    "zone level sk": "Zone_Level_SK",
    "state level sk": "State_Level_SK",
    "city level sk" : "City_Level_SK",
    "sol sk": "SalesOffice_Level_SK",  
    "sot": "Sales Offcie Type",  
    "solsk": "SalesOffice_Level_SK",
    "Toned Milk":"TM",
    "toned milk":"TM",
    "tg 2":"TG-2",
    "hyd":"consider as HYDERABAD",
    "kothakoata":"B.KOTHAKOTA",
    "ap 1": "AP-1",
    "ap 2": "AP-2",
    "ap 3": "AP-3",
    "AP 1": "AP-1",
    "AP 2": "AP-2",
    "TG 1": "TG-1",
    "tg 1": "TG-1",
    "KA": "KA",
    "z4": "Z-4",
    "z 4": "Z-4",
    "Z 4": "Z-4",
    "Tamil nadu":"TAMIL NADU",
    "tamil nadu":"TAMIL NADU",
    "TN":"TAMIL NADU",
    "PUNJAB":"PUNJAB",
    "punjab":"PUNJAB", 
    "GUJARAT":"GUJARAT",
    "gujarat":"GUJARAT",
    "HARYANA":"HARYANA",
    "haryana":"HARYANA",
    "harayana":"HARYANA",
    "hariyana":"HARYANA",
    "RAJASTHAN":"RAJASTHAN",
    "rajasthan":"RAJASTHAN",
    "NIZAMABAD":"NIZAMABAD",
    "nizamabad":"NIZAMABAD",
    "RAI":"RAI",
    "rai":"RAI",
    "chennai sales office 5":"Chennai Sales Office - 5",
    "chennai sales office 3":"Chennai Sales Office - 3",
    "bangalore sales office 1":"Bangalore Sales Office - 1",
    "export sales office KA":"Export Sales Office - KA",
    "coimbattore sales office":"COIMBATTORE SALES OFFICE",
    "HYDERABAD-2":"HYDERABAD-2",
    "hyd 2":"HYDERABAD-2",
    "HYDERABAD-1":"HYDERABAD-1",
    "hyd 1":"HYDERABAD-1",
    "ytd":"[ytd means you have to calculate like this: ERE BillingDate >= CASE WHEN MONTH(GETDATE()) >= 4 THEN CAST(YEAR(GETDATE()) AS VARCHAR) + '-04-01' ELSE CAST(YEAR(GETDATE()) - 1 AS VARCHAR) + '-04-01' END AND BillingDate <= CAST(GETDATE() - 1 AS DATE)]",
    "mtd": "month to date",
    "qtd": "quarter to date",
    "wtd": "week to date",
    "last 7 days": "last 7 days",
    "total volume": "[volume means SUM(SalesQuantity) / number_of_days_in_time_period, where number_of_days_in_time_period = total full days in the query date range] and only when user mentioned any product from any ProductHeirachy or material group MUST include this also: MAX(f.SalesUnit) AS SalesUnit]",
    "sales": "[sales means SUM(SalesQuantity) / number_of_days_in_time_period, where number_of_days_in_time_period = total full days in the query date range]  and only when user mentioned any product from any ProductHeirachy or material group MUST include this also: MAX(f.SalesUnit) AS SalesUnit]",
    "change":"DELTA",
    "difference":"DELTA",
    "mom":"month on month",
    "tso":"TSO",
    "sale":"[sale means SUM(SalesQuantity) / number_of_days_in_time_period, where number_of_days_in_time_period = total full days in the query date range] and only when user mentioned any product from any ProductHeirachy or material group MUST include this also: MAX(f.SalesUnit) AS SalesUnit] regardless of any other filters like location or time period.",
    "year on year":"YoY",
    "day on day":"DoD",
    "mom":"MoM",
    "yoy":"YoY",
    "dod":"DoD",
    "volume":"[volume means SUM(SalesQuantity) / number_of_days_in_time_period, where number_of_days_in_time_period = total full days in the query date range] and only when user mentioned any product from any ProductHeirachy or material group MUST include this also: MAX(f.SalesUnit) AS SalesUnit]",
    "sales office id ":"SalesOfficeID ",
    "material code":"MaterialCode",
    "current month": "this month till yesterday",
    "seasonality": "sesonality months",
    "season": "top 3 months with more sale",
    "icecream/fd":"Materialgroup",
    "agents":"[you will find agents in CustomerGroup]",
    "hdc":"[you will find hdc in CustomerGroup]",
    "Parlours":"you will find parlours in CustomerGroup",
    "salesoffice": "consider both Short_Name or plant from DW.dSalesOfficeMaster ",
    "sales office": "consider both Short_Name or plant from DW.dSalesOfficeMaster ",
    "sos":"consider both Short_Name from DW.dSalesOfficeMaster",
    "net realization":"consider products from ProductHeirachy1",
    "nrv":"net realization, consider products from ProductHeirachy1",
    "Chennai Sales Office - 1":	"CSO 1","Chennai Sales Office - 2":	"CSO 2",
"Vadamadurai Sales Office-3": "VMISO",
"Chennai Sales Office - 6": "CSO 6",  
"Chennai Sales Office - 4": "CSO 4",
"Chennai Sales Office - 3": "CSO 3",
"Chennai Sales Office - 5": "CSO 5",
"Export Sales Office - TN": "EXPORT SALES - TN",
"Salem Sales Office": "SALEM SO",
"Dindigul Sales Office": "DINDIGUL SO",
"Vellore Sales Office": "VELLORE SO",
"Chennai Sales Office - 8": "CSO 8",
"Bangalore Sales Office - 1": "BSO 1",
"Bangalore Sales Office - 2": "BSO 2",
"Bangalore Sales Office - 3": "BSO 3",
"Bangalore Sales Office - 6": "BSO 6",
"Export Sales Office - KA": "EXPORT SALES - KA",
"Bangalore Sales Office - 4": "BSO 4",
"Bangalore Sales Office - 8": "BSO 8",
"Sindhanur Sales Office": "SDNSO",
"Hyderabad Sales Office - 2": "HSO 2",
"Hyderabad Sales Office - 4": "HSO 4",
"Hyderabad Sales Office - 5": "HSO 5",
"Hyderabad Sales Office - 6": "HSO 6",
"Hyderabad Sales Office - 3": "HSO 3",
"Hyderabad Sales Office - 7": "HSO 7",
"Export Sales Office - AP": "EXPORT SALES OFFICE - AP",
"Narketpally Sales Office": "NKPSO",
"Export Sales Office - TS": "EXPORT SALES OFFICE - TS",
"Hyderabad Sales Office - 8": "HSO 8",
"Hyderabad Sales Office - 9": "HSO 9",
"Hyderabad Sales Office - 10": "HSO 10",
"Hyderabad Sales Office - 11": "HSO 11",
"Hyderabad Sales Office - 12": "HSO 12",
"Hyderabad Sales Office - 13": "HSO 13",
"Vizag Sales Office": "VSO",
"Bobbili Sales Office": "BBLSO",
"Pamarru Sales Office": "PMSO",
"Tanuku Sales Office": "TNKSO",
"Rajahmundry Sales Office": "RSO",
"Kakinada Sales Office": "KKDSO",
"Pamarru Sales Office-6": "PMSO 6",
"Vizag Sales Office-6": "VSO 6",
"Pamarru Sales Office-8": "PMSO 8",
"Vizag Sales Office-8": "VSO 8",
"Vijayawada Sales Office-8": "VJASO 8",
"Kalluru Sales Office": "KLRSO",
"Battiprolu Sales office": "VJASO",
"Vijayawada Sales Office": "VJASO",
"Vijayawada Sales Office-6": "VJASO 6",
"Tirupati Sales Office": "TSO",
"Chittoor Sales Office": "CTRSO",
"Gokul Sales Office": "GSO",
"Gokul Sales Office-8": "GSO 8",
"Mumbai Sales Office": "MSO",
"Pune Sales Office": "PSO",
"Sangvi Sales Office": "SNVSO",
"Sangvi Trading Sales Office": "SNVSO trade",
"Mumbai Sales Office 6": "MSO 6",
"Export Sales Office - MH": "EXPORT SALES OFFICE - MH",
"Mumbai Sales Office - 7": "MSO 7",
"Mumbai Sales Office - 2": "MSO 2",
"Bhandara Sales Office": "BHANDARA SO",
"Vasai (Thane) Sales Office": "VASAI SO",
"Pune Sales Office1(Dhankawadi)": "PUNE SO1",
"Mumbai Sales Office-8": "MSO 8",
"Delhi Sales Office": "DSO",
"Rai Sales Office": "RAI SO",
"Delhi Sales Office - 6": "DSO 6",
"Rai Sales Office - 6": "RAI SO 6",
"Kundli Sales Office": "KUNDLI SO",
"Rai Sales Office 1": "RAI SO 1",
"Rai Sales Office 2": "RAI SO 2",
"Rai Sales Office 3": "RAI SO 3",
"Rai Sales Office 4": "RAI SO 4",
"Kaladera Sales Office": "KALADERA SO",
"Jaipur Sales Office": "JAIPUR SO",
"Ludhiana Sales Office": "LUDHIANA SO",
"Khamanon Sales Office": "KMNO SO",
"Morena Sales Office": "MORENA SO",
"Ghaziabad Sales Office": "Ghazizbad SO",
"Uttarakhand Sales Office": "Uttarakhand SO",
"ONGOLE SO": "ONGOLE SO",
"Cochin sales office-so": "Cochin sales office-so",
"Traimbakam- Sales Office": "Brahmpur - So",
"Kolkatta Sales Office": "Kolkatta Sales Office",
"HYDERABAD SALES OFFICE - 14 - Catg-2": "HSO 14",
"BOBBILI SALES OFFICE-6": "BBLSO 6",
"VIZAG SALES OFFICE-2": "VSO 2",
"GOKUL SALES OFFICE-6": "GSO 6",
"CHITTOOR SALES OFFICE-6": "CTRSO 6",
"MANOR SALES OFFICE": "MNSO",
"Guntur Sales Office": "GNTSO",
"Gollapudi Sales Office": "GLPSO",
"Mangalagiri Sales Office": "MGLSO",
"Kolkatta Sales Office": "KLKSO",
"RAIPUR SO (C&F)": "RAIPUR",
"Indore Sales Office (C&F)": "INDORE",
"Patna Sales Office (C&F)": "PATNA",
"Pune Sales Office (C&F)": "PUNE",
"Cuttack Sales Office (C&F)": "CUTTACK",
"Cochin Sales Office (C&F)": "Cochin",
"Nagpur": "NAGPUR",
"SHAMIRPET SALES OFFICE": "SMPTS",
"Vadamadurai Sales Office 6": "VMISO-6",
"Hyd Sales Office - 17 (KA)": "HSO 17(KA)",
"Bhubaneswar Sales Office": "BHUBANESHWER SO",
"Thane Sales Office": "THANE SO",
"Hyderabad SO UP Ctry - 8": "HSO 8 UP CTRY",
"Gokul SO - 2 (ATPSO)": "GSO 2(ATPSO)",
"Hubli SO": "HUBLI SO",
"ERODE": "ERODE SO",
"Hyderabad Sales Office - 15": "HSO 15",
"Bangalore Sales Office - 5": "BSO 5",
"B.KOTHAKOTA SALES OFFICE": "BKKSO",
"LUCKNOW": "LUCKNOW SO",
"BIDAR": "BIDAR SO",
"Kanchipuram": "KANCHIPURAM SO",
"Orissa Sales Office": "ORISSA",
"Nellore Sales office": "NELLORE SO2",
"MP Sales office": "MPSO",
"Nellore Sales office": "NELLORE SO",
"COIMBATTORE SALES OFFICE": "COIMBATTORE SO",
"flav milk":"ProductHeirachy1 = 'flav.milk'",
"flavoured milk":"ProductHeirachy1 = 'flav.milk'",
"billed customers":"billed customers means calculate ubc dont consider billed customers as a customergroup",
"DNE":"SELECT COUNT(DISTINCT CustomerID) FROM DW.fSales",
"budget":"display actual, target, achievement",
"VAP":"calculate based on net realization",
"value added products":"calculate net realization",
"VS": "[vs means Display the data for both sides of the comparison when 'vs' is mentioned, such as both months, products, or periods.]",
"vs": "[vs Display the data for both sides of the comparison when 'vs' is mentioned, such as both months, products, or periods.]",
"Vs": "[vs Display the data for both sides of the comparison when 'vs' is mentioned, such as both months, products, or periods.]",
"ml":"[ml means must consider ProductHeirachy5 also]",
"GMS":"[GMS means must consider ProductHeirachy5 also]",
"KG": "[KG means must consider ProductHeirachy5 also]",
"Liters":"[Liters means must consider ProductHeirachy5 also]",
"liters":"[liters means must consider ProductHeirachy5 also]",
"kg": "[Kg means must consider ProductHeirachy5 also]",
"gms":"[gms means must consider ProductHeirachy5 also]",
"January"   : "[january means consider 31 days]",
"February"  : "[February means 28 days or 29.0 for leap years]",
"March" : "[March means consider 31 days]",
"April"     : "[April means consider  30 days]",
"May"       : "[May means consider 31 days]",
"June"      : "[june means consider 30 days]",
"July"      : "[July means consider 31 days]",
"August"    : "[August means consider 31 days]",
"September" : "[September means consider 30 days]",
"October"   : "[October means consider 31 days]",
"November"  : "[November means consider 30 days]",
"December"  : "[December means consider 31 days]",
"customers":"customers means ubc:COUNT(DISTINCT CustomerID)",
"till date":"but consider mtd..month to date",
"toned milk":"consider both milk and tm, like : AND ProductHeirachy1 = 'Milk' AND ProductHeirachy3 = 'TM'",
"buffalo milk":"buffalo milk means BFCM in ProductHeirachy3 ",
"toned":"milk and tm",
"General Trade":"[General Trade means, in CustomerGroup column dont consider these: E-Commerce,E-Commerce,Modern Formats,MRF Distributor]",
"GT":"[GT means, in CustomerGroup column don't consider these i mean exclude these: E-Commerce,E-Commerce,Modern Formats,MRF Distributor]",
"NKA":"[If the user says NKA, do not use REGION_NAME = 'NKA'. Instead,  in CustomerGroup column Only consider these: E-Commerce,E-Commerce,Modern Formats,MRF Distributor, these all find in CustomerGroup]",
"lastweek":"[for last week close brackets correctly: DATEADD(DAY, -7, DATEADD(WEEK, DATEDIFF(WEEK, 0, GETDATE()), 0)) AND DATEADD(DAY, -1, DATEADD(WEEK, DATEDIFF(WEEK, 0, GETDATE()), 0)) and also lastweek means 7 days period]",
"package size":"[package size means you must consider ProductHeirachy5 values along with user asked values]",
"package type":"[package type means you must consider ProductHeirachy4 values along with user asked values]",
"fastest growing":"[here fastest growing means top performing]",
"slowest growing":"[here slowest growing means low performing]",
"buffalo":"[buffalo means you has to strictly map to 'BFCM' in ProductHeirachy3]",
"buffalomilk":"[buffalo means you has to strictly map to 'BFCM' in ProductHeirachy3]",

}

def replace_business_terms(user_input: str) -> str:
    for key, value in business_term_mapping.items():
        pattern = re.compile(r'\b' + re.escape(key) + r'\b', re.IGNORECASE)
        user_input = pattern.sub(value, user_input)
    temp = "for every query include this: WHERE ProductHeirachy1 NOT IN ('Feed', 'Feed Supplement', 'Veterinary Medicines', 'Water', 'HERITA LIV', 'NULL', 'others') || Strict Domain Interpretation and SQL Mapping Rules:\n"
    return temp + user_input

from langchain.prompts import PromptTemplate


prompt_template_sale = PromptTemplate(
    input_variables=["user_input"], 
    template=""" 
Role: You are an expert SQL generator and business analyst for a BI chatbot.

Goal: Convert natural language into accurate, efficient T-SQL using schema and business logic.
- Generalize from examples — extract logic, don’t memorize.
- Use reasoning to infer missing or implicit details.
- Follow business rules, fiscal calendar, join paths, and naming conventions.
- Output queries that are complete, correct, and production-ready.
 
    > tables_and_schema: > Use the following tables and their core columns:
        # Table 1: DW.fSales
            - BillingDocument (bigint)
            - BillingDocumentItem (int)
            - BillingDate (date)
            - SalesOfficeID (int)
            - DistributionChannel (nvarchar)
            - CustomerGroup (nvarchar)
            - CustomerID (nvarchar)
            - ProductHeirachy1 (nvarchar)
            - ProductHeirachy2 (nvarchar)
            - ProductHeirachy3 (nvarchar)
            - ProductHeirachy4 (nvarchar)
            - ProductHeirachy5 (nvarchar)
            - ProductHeirachy5 (nvarchar)
            - Materialgroup (nvarchar)
            - SubMaterialgroup1 (nvarchar)
            - SubMaterialgroup2 (nvarchar)
            - SubMaterialgroup3 (nvarchar)
            - MaterialCode (int)
            - SalesQuantity (decimal)
            - SalesUnit (nvarchar)
            - nvarchar (decimal)
            - TotalTax (decimal)
            - NetAmount (decimal)
            - IsActive (bit)
        # Table 2: DW.dSalesOfficeMaster
            - PLANT (float)
            - PLANT_NAME (nvarchar)
            - CITY (nvarchar)
            - STATE (nvarchar)
            - Sales_Zone (nvarchar)
            - REGION_NAME (nvarchar)
            - AREA_NAME (nvarchar)
            - Short_Name (nvarchar)
            - [Sales Offcie Type] (nvarchar) = 'sales Office'
            - Sales Offcie Type (nvarchar)
            - State_Level_SK (nvarchar)
            - City_Level_SK (nvarchar)
            - SalesOffice_Level_SK (nvarchar)
        # Table 3: DW.fSalesBudget
            - SalesOfficeID (int)
            - MaterialCode (int)
            - FinancialYear (int)
            - PlanMonth (int)
            - BudgetQuantityLPD (decimal)
        #Table 4: [worddb].[HFLSQLReader].[customer_Mappingnames]
            - CustomerID	(varchar)
            Customer_name	(varchar)
    IMPORTANT NOTE: The columns SalesOfficeID (integer) in the DW.fSales table, PLANT (float) in the DW.dSalesOfficeMaster table, and SalesOfficeID (integer) in the DW.fSalesBudget table all represent the same data. Although their names and data types differ across these tables, they refer to the same entity and can be used to join these tables together.
    
    > joins_and_mapping_rules: > 
        - join DW.fSales with DW.dSalesOfficeMaster like : select * from dw.fSales f inner join DW.dSalesOfficeMaster d on f.SalesOfficeID = d.PLANT and d.[Sales Offcie Type]='sales Office'
        - join DW.fSalesBudget with DW.dSalesOfficeMaster like: select * from DW.fSalesBudget b inner join DW.dSalesOfficeMaster d on b.SalesOfficeID=d.plant and d.[Sales Offcie Type]='sales Office'
        - join  [worddb].[HFLSQLReader].[customer_Mappingnames] as like : LEFT JOIN [worddb].[HFLSQLReader].[customer_Mappingnames] example:  FROM Dw.fsales f JOIN Dw.dSalesOfficeMaster d ON f.SalesOfficeID = d.PLANT LEFT JOIN [worddb].[HFLSQLReader].[customer_Mappingnames]
        
    > date_logic_and_fiscal_rules: Everytime follow these logics:
        - ytd or year to date logic:Financial Year Start: April 1 : WHERE BillingDate >= CASE WHEN MONTH(GETDATE()) >= 4 THEN CAST(YEAR(GETDATE()) AS VARCHAR) + '-04-01' ELSE CAST(YEAR(GETDATE()) - 1 AS VARCHAR) + '-04-01' END AND BillingDate <= CAST(GETDATE() - 1 AS DATE)
        - qtd or quarter to date logic: WHERE BillingDate >= CASE WHEN MONTH(GETDATE()) BETWEEN 4 AND 6 THEN CAST(YEAR(GETDATE()) AS VARCHAR) + '-04-01' WHEN MONTH(GETDATE()) BETWEEN 7 AND 9 THEN CAST(YEAR(GETDATE()) AS VARCHAR) + '-07-01' WHEN MONTH(GETDATE()) BETWEEN 10 AND 12 THEN CAST(YEAR(GETDATE()) AS VARCHAR) + '-10-01' ELSE CAST(YEAR(GETDATE()) AS VARCHAR) + '-01-01' END AND BillingDate <= CAST(GETDATE() - 1 AS DATE) 
        - mtd or month to date logic: Start of current month to yesterday, eg:  WHERE BillingDate >= DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1) AND BillingDate <= CAST(GETDATE() - 1 AS DATE) or ** 
        - last week or previous week logic: BETWEEN DATEADD(DAY, -7, DATEADD(WEEK, DATEDIFF(WEEK, 0, GETDATE()), 0)) AND DATEADD(DAY, -1, DATEADD(WEEK, DATEDIFF(WEEK, 0, GETDATE()), 0))
        - Days in Each Month (Fixed): Jan–31, Feb–28 (29 in leap years), Mar–31, Apr–30, May–31, Jun–30, Jul–31, Aug–31, Sep–30, Oct–31, Nov–30, Dec–31.
        - lastmonth:  EOMONTH(DATEADD(MONTH, -1, GETDATE()))
        - Completed Days in Current Month: DAY(GETDATE()) - 1
        - sale quantity or volume or sale : Always calculate the SUM(SalesQuantity)/(time period)
        - opm or over previous month logic: WHERE BillingDate >= DATEFROMPARTS(YEAR(DATEADD(MONTH, -1, GETDATE())), MONTH(DATEADD(MONTH, -1, GETDATE())), 1) AND BillingDate <= EOMONTH(DATEADD(MONTH, -1, GETDATE()))
        - opy or over previous year logic: WHERE BillingDate >= DATEFROMPARTS(YEAR(DATEADD(YEAR, -1, GETDATE())), MONTH(GETDATE()), 1) AND BillingDate <= EOMONTH(DATEFROMPARTS(YEAR(DATEADD(YEAR, -1, GETDATE())), MONTH(GETDATE()), 1))

    > INSTRUCTIONS:
        - understand the user question properly, use date_logic_and_fiscal_rules for calculating dates etc.
        - learn concept through examples and generate ssms sql query by understanding user query properly.
        very imp: Whenever the query contains location filters such as CITY, STATE, REGION, PLANT_NAME, Short_Name then SQL must join DW.fSales (f) with DW.dSalesOfficeMaster (d) using f.SalesOfficeID = d.PLANT and filter on the location fields from d. Additionally, always include d.[Sales Offcie Type] = 'sales Office'
        "SalesQuantity Logic: If the question refers to a single specific day (e.g., 'yesterday', 'on 5th June 2025'), use the total SalesQuantity without dividing. If the question refers to a date range (e.g., 'last week', 'MTD', 'QTD', 'YTD', or two dates), then divide the total SalesQuantity by the number of days in that range to calculate the daily average — do not assume 1 day for ranges."
        This makes it clear: # Single day → no division #Range → divide by actual number of days #Never assume 1 when it's a range
    **"livo": "[livo means strictly you have to map like this: SELECT SUM(SalesQuantity) FROM DW.fSales WHERE MaterialCode IN ('30769', '30704', '30706')]",**
    NOTE : Salesoffice means consider  like(select PLANT_NAME or short_name from DW.dSalesOfficeMaster)
    NOTE : Region means consider like (select REGION_NAME from DW.dSalesOfficeMaster)

    MAIN LOGIC:
       #usecase 1: sale quantity or volume or sale calculation:  
    examples:
        eg 1) what is the yesterday sale ? SQL: SELECT SUM(SalesQuantity) * 1.0 / 1 AS TotalSalesYesterday FROM DW.fSales WHERE BillingDate = CAST(GETDATE() - 1 AS DATE) AND ProductHeirachy1 NOT IN ('Feed', 'Feed Supplement', 'Veterinary Medicines', 'Water', 'HERITA LIV', 'NULL', 'others');
        eg 2) What was the sale of Milk for HSO 1 in last month ? SQL :SELECT SUM(f.SalesQuantity) * 1.0 / DAY(EOMONTH(DATEADD(MONTH, -1, GETDATE()))) AS MilkSales_LastMonth, MAX(f.SalesUnit) AS SalesUnit FROM DW.fSales f INNER JOIN DW.dSalesOfficeMaster d ON f.SalesOfficeID = d.PLANT WHERE d.Short_Name = 'HSO 1' AND d.[Sales Offcie Type] = 'sales Office' AND f.ProductHeirachy1 = 'Milk' AND f.ProductHeirachy1 NOT IN ('Feed', 'Feed Supplement', 'Veterinary Medicines', 'Water', 'HERITA LIV', 'NULL', 'others') AND f.BillingDate >= DATEFROMPARTS(YEAR(DATEADD(MONTH, -1, GETDATE())), MONTH(DATEADD(MONTH, -1, GETDATE())), 1) AND f.BillingDate <= EOMONTH(DATEADD(MONTH, -1, GETDATE()));
            -- when they asked about the product you has to display sales quantity along with its 'SalesUnit' like above example
        eg 3) what is the sale for milk yesterday? SQL:SELECT SUM(f.SalesQuantity) AS SaleQuantity_Yesterday, MAX(f.SalesUnit) AS SalesUnit FROM DW.fSales f JOIN DW.dSalesOfficeMaster d ON f.SalesOfficeID = d.PLANT WHERE f.ProductHeirachy1 = 'Milk' AND CAST(f.BillingDate AS DATE) = CAST(GETDATE() - 1 AS DATE) AND d.[Sales Offcie Type] = 'sales Office';

        #usecase 2:top performing(descending order), bottom performing(ascending order)
        eg 3) what are the bottom performing or less performing states based on the sale of april 2025 ? SQL: SELECT TOP 5 d.STATE, SUM(f.SalesQuantity) * 1.0 / 30 AS AvgSalesPerDay FROM DW.fSales f INNER JOIN DW.dSalesOfficeMaster d ON f.SalesOfficeID = d.PLANT WHERE f.BillingDate >= '2025-04-01' AND f.BillingDate <= '2025-04-30' AND d.[Sales Offcie Type] = 'sales Office' AND f.ProductHeirachy1 NOT IN ('Feed', 'Feed Supplement', 'Veterinary Medicines', 'Water', 'HERITA LIV', 'NULL', 'others') GROUP BY d.STATE ORDER BY AvgSalesPerDay;

        
    > strict Instruction to LLM: "When generating SQL queries, always think critically and contextually based only on the user’s query.   dynamically analyze the user intent and introduce or modify columns, filters, or logic as required by the specific query. Every output should be tailored precisely to the query's needs, integrating only the relevant dimensions, measures, and date logic."

    > edge case terms to handle:
        • If the user query mentions "icecream/fd", "ice cream/fd", "frozen dessert", or any variation that explicitly includes "fd":
             → Then use: Materialgroup = 'ICE CREAM/FD'
        (Make sure it's in uppercase and enclosed in single quotes.)
        • If the user query mentions only "icecream" or "ice cream" without "fd" or "frozen dessert":
            → Then apply filter using Product Hierarchy only (e.g., ProductHeirachy1 to ProductHeirachy5), based on context or available values.
        • Do not use both Product Hierarchy and Materialgroup for "ice cream" unless explicitly mentioned.
        • If the user query mentions "flav.milk' or 'flavoured milk' or 'flav milk' then Consider: ProductHeirachy1 = 'flav.milk'
         'flav.milk' and 'milk' are different but both present in the ProductHeirachy1 search carefully
        IMP: example: user query: what is the sale of toned milk yesterday: SELECT SUM(SalesQuantity)/1 AS MilkSalesQuantity_Yesterday FROM DW.fSales WHERE ProductHeirachy1 = 'Milk' AND BillingDate = CAST(GETDATE() - 1 AS DATE) AND ProductHeirachy1 NOT IN ('Feed', 'Feed Supplement', 'Veterinary Medicines', 'Water', 'HERITA LIV', 'NULL', 'others')
        -- Toned milk means you has to consider milk and milk tm
    
    "I'll tip you $200 if you learn the concept from the examples above and generate an accurate SQL query that satisfies the user input"

    > User Query:{user_input}

    SQL:
    Give direct ssms executable query without any explanations  

""")

prompt_template_ubc = PromptTemplate(
    input_variables=["user_input"], 
    template=""" 
Role: You are an expert SQL generator and business analyst for a BI chatbot.

Goal: Convert natural language into accurate, efficient T-SQL using schema and business logic.
- Generalize from examples — extract logic, don’t memorize.
- Use reasoning to infer missing or implicit details.
- Follow business rules, fiscal calendar, join paths, and naming conventions.
- Output queries that are complete, correct, and production-ready.

    
    > tables_and_schema: > Use the following tables and their core columns:
        # Table 1: DW.fSales
            - BillingDocument (bigint)
            - BillingDocumentItem (int)
            - BillingDate (date)
            - SalesOfficeID (int)
            - DistributionChannel (nvarchar)
            - CustomerGroup (nvarchar)
            - CustomerID (nvarchar)
            - ProductHeirachy1 (nvarchar)
            - ProductHeirachy2 (nvarchar)
            - ProductHeirachy3 (nvarchar)
            - ProductHeirachy4 (nvarchar)
            - ProductHeirachy5 (nvarchar)
            - ProductHeirachy5 (nvarchar)
            - Materialgroup (nvarchar)
            - SubMaterialgroup1 (nvarchar)
            - SubMaterialgroup2 (nvarchar)
            - SubMaterialgroup3 (nvarchar)
            - MaterialCode (int)
            - SalesQuantity (decimal)
            - SalesUnit (nvarchar)
            - nvarchar (decimal)
            - TotalTax (decimal)
            - NetAmount (decimal)
            - IsActive (bit)
        # Table 2: DW.dSalesOfficeMaster
            - PLANT (float)
            - PLANT_NAME (nvarchar)
            - CITY (nvarchar)
            - STATE (nvarchar)
            - Sales_Zone (nvarchar)
            - REGION_NAME (nvarchar)
            - AREA_NAME (nvarchar)
            - Short_Name (nvarchar)
            - [Sales Offcie Type] (nvarchar) = 'sales Office'
            - Sales Offcie Type (nvarchar)
            - State_Level_SK (nvarchar)
            - City_Level_SK (nvarchar)
            - SalesOffice_Level_SK (nvarchar)
        # Table 3: DW.fSalesBudget
            - SalesOfficeID (int)
            - MaterialCode (int)
            - FinancialYear (int)
            - PlanMonth (int)
            - BudgetQuantityLPD (decimal)
        #Table 4: [worddb].[HFLSQLReader].[customer_Mappingnames]
            - CustomerID	(varchar)
            Customer_name	(varchar)
    IMPORTANT NOTE: The columns SalesOfficeID (integer) in the DW.fSales table, PLANT (float) in the DW.dSalesOfficeMaster table, and SalesOfficeID (integer) in the DW.fSalesBudget table all represent the same data. Although their names and data types differ across these tables, they refer to the same entity and can be used to join these tables together.
    
    > joins_and_mapping_rules: > 
        - join DW.fSales with DW.dSalesOfficeMaster like : select * from dw.fSales f inner join DW.dSalesOfficeMaster d on f.SalesOfficeID = d.PLANT and d.[Sales Offcie Type]='sales Office'
        - join DW.fSalesBudget with DW.dSalesOfficeMaster like: select * from DW.fSalesBudget b inner join DW.dSalesOfficeMaster d on b.SalesOfficeID=d.plant and d.[Sales Offcie Type]='sales Office'
        - join  [worddb].[HFLSQLReader].[customer_Mappingnames] as like : LEFT JOIN [worddb].[HFLSQLReader].[customer_Mappingnames] example:  FROM Dw.fsales f JOIN Dw.dSalesOfficeMaster d ON f.SalesOfficeID = d.PLANT LEFT JOIN [worddb].[HFLSQLReader].[customer_Mappingnames]

    > date_logic_and_fiscal_rules: >
        - ytd or year to date logic:Financial Year Start: April 1 : WHERE BillingDate >= CASE WHEN MONTH(GETDATE()) >= 4 THEN CAST(YEAR(GETDATE()) AS VARCHAR) + '-04-01' ELSE CAST(YEAR(GETDATE()) - 1 AS VARCHAR) + '-04-01' END AND BillingDate <= CAST(GETDATE() - 1 AS DATE)
        - qtd or quarter to date logic: WHERE BillingDate >= CASE WHEN MONTH(GETDATE()) BETWEEN 4 AND 6 THEN CAST(YEAR(GETDATE()) AS VARCHAR) + '-04-01' WHEN MONTH(GETDATE()) BETWEEN 7 AND 9 THEN CAST(YEAR(GETDATE()) AS VARCHAR) + '-07-01' WHEN MONTH(GETDATE()) BETWEEN 10 AND 12 THEN CAST(YEAR(GETDATE()) AS VARCHAR) + '-10-01' ELSE CAST(YEAR(GETDATE()) AS VARCHAR) + '-01-01' END AND BillingDate <= CAST(GETDATE() - 1 AS DATE) 
        - mtd or month to date logic: Start of current month to yesterday, eg:  WHERE BillingDate >= DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1) AND BillingDate <= CAST(GETDATE() - 1 AS DATE)
        - last week or previous week logic: BETWEEN DATEADD(DAY, -7, DATEADD(WEEK, DATEDIFF(WEEK, 0, GETDATE()), 0)) AND DATEADD(DAY, -1, DATEADD(WEEK, DATEDIFF(WEEK, 0, GETDATE()), 0))
        - Days in Each Month (Fixed): Jan–31, Feb–28 (29 in leap years), Mar–31, Apr–30, May–31, Jun–30, Jul–31, Aug–31, Sep–30, Oct–31, Nov–30, Dec–31.
        - lastmonth:  EOMONTH(DATEADD(MONTH, -1, GETDATE()))
        - Completed Days in Current Month: DAY(GETDATE()) - 1
        - sale quantity or volume or sale : Always calculate the SUM(SalesQuantity)/(time period)
        - opm or over previous month logic: WHERE BillingDate >= DATEFROMPARTS(YEAR(DATEADD(MONTH, -1, GETDATE())), MONTH(DATEADD(MONTH, -1, GETDATE())), 1) AND BillingDate <= EOMONTH(DATEADD(MONTH, -1, GETDATE()))
        - opy or over previous year logic: WHERE BillingDate >= DATEFROMPARTS(YEAR(DATEADD(YEAR, -1, GETDATE())), MONTH(GETDATE()), 1) AND BillingDate <= EOMONTH(DATEFROMPARTS(YEAR(DATEADD(YEAR, -1, GETDATE())), MONTH(GETDATE()), 1))
       
    > INSTRUCTIONS:
        - understand the user question properly, use date_logic_and_fiscal_rules for calculating dates etc.
        ** very important: if any one asked question relating to the these columns : CITY,STATE,REGION_NAME,Short_Name then join like - join DW.fSales with DW.dSalesOfficeMaster like : select * from dw.fSales f inner join DW.dSalesOfficeMaster d on f.SalesOfficeID = d.PLANT and d.[Sales Offcie Type]='sales Office'
        - learn concept through Main logic examples and generate ssms sql query by understanding user query properly.

    MAIN LOGIC:
    **Important Note: The following examples cover common ways users may ask about UBC  If a user query differs in wording or format, interpret the intent and generate the SQL by adapting the logic shown in these examples. Always use them as reference patterns for solving similar queries.**

    =>usecase 1: 
    MODEL 1: UNIQUE BILLING COUNT OR UNIQUE BILLING CUSTOMERS: -> ubc or unique billing count or unique billing customers:
        meaning: Number of unique customers based on the CustomerID column in Dw.fsales table.
        eg 1)what is unique billing count yesterday?  -> sql query: SELECT COUNT(DISTINCT BillingDocument) AS UniqueBillingCount_Yesterday FROM Dw.fsales WHERE CAST(BillingDate AS DATE) = CAST(GETDATE() - 1 AS DATE);
        eg 2) what is dne yesterday?  -> sql query: SELECT COUNT(DISTINCT BillingDocument) AS UniqueBillingCount_Yesterday FROM Dw.fsales WHERE CAST(BillingDate AS DATE) = CAST(GETDATE() - 1 AS DATE);
        eg 3) what is unique billed customers yesterday ? -> SQl: SELECT COUNT(DISTINCT BillingDocument) AS UniqueBillingCount_Yesterday FROM Dw.fsales WHERE CAST(BillingDate AS DATE) = CAST(GETDATE() - 1 AS DATE);

    MODEL 2: what is ubc for hyd city yesterday: sql query-> SELECT COUNT(DISTINCT CustomerID) FROM DW.fSales f JOIN DW.dSalesOfficeMaster d ON f.SalesOfficeID = d.PLANT AND d.[Sales Offcie Type] = 'sales Office' WHERE d.CITY = 'HYderabad' AND f.BillingDate = CAST(GETDATE() - 1 AS DATE) AND ProductHeirachy1 NOT IN ('Feed', 'Feed Supplement', 'Veterinary Medicines', 'Water', 'HERITA LIV', 'NULL', 'others');
    THIS is like if any one asked question relating to the these columns : CITY,STATE,REGION_NAME,Short_Name then join like - join DW.fSales with DW.dSalesOfficeMaster like : select * from dw.fSales f inner join DW.dSalesOfficeMaster d on f.SalesOfficeID = d.PLANT and d.[Sales Offcie Type]='sales Office'

    MODEL 3: If customer group was mentioned: 
        eg 3) what is ubc of HDC yesterday? -> sql query: SELECT COUNT(DISTINCT CustomerID) FROM DW.fSales WHERE CustomerGroup = 'HDC' AND BillingDate = CAST(GETDATE() - 1 AS DATE) AND ProductHeirachy1 NOT IN ('Feed', 'Feed Supplement', 'Veterinary Medicines', 'Water', 'HERITA LIV', 'NULL', 'others')
        **below was the Different types of asking a same question: 
    NOTE: ** TOP - we will decide a customer as top based on the Avg Sale
    **below was the Different types of asking a same question: 
        eg 4) who are top 5 customers for agents yesterday: SELECT f.CustomerID, ISNULL(cm.Customer_name, 'UNKNOWN') AS Customer_name, d.Short_Name, SUM(f.SalesQuantity) * 1.0 / NULLIF(COUNT(DISTINCT f.BillingDate), 0) AS Metric FROM Dw.fsales f JOIN Dw.dSalesOfficeMaster d ON f.SalesOfficeID = d.PLANT AND d.[Sales Offcie Type] = 'sales Office' LEFT JOIN [worddb].[HFLSQLReader].[customer_Mappingnames] cm ON TRY_CAST(f.CustomerID AS INT) = TRY_CAST(cm.CustomerID AS INT) WHERE BillingDate = CAST(GETDATE() - 1 AS DATE) AND CustomerGroup = 'Agents' GROUP BY f.CustomerID, cm.Customer_name, d.Short_Name ORDER BY Metric DESC OFFSET 0 ROWS FETCH NEXT 5 ROWS ONLY;
        eg 5) who are top 5 billed customers from agents yesterday ? -> sql query: SELECT f.CustomerID, ISNULL(cm.Customer_name, 'UNKNOWN') AS Customer_name, d.Short_Name, SUM(f.SalesQuantity) * 1.0 / NULLIF(COUNT(DISTINCT f.BillingDate), 0) AS Metric FROM Dw.fsales f JOIN Dw.dSalesOfficeMaster d ON f.SalesOfficeID = d.PLANT AND d.[Sales Offcie Type] = 'sales Office' LEFT JOIN [worddb].[HFLSQLReader].[customer_Mappingnames] cm ON TRY_CAST(f.CustomerID AS INT) = TRY_CAST(cm.CustomerID AS INT) WHERE BillingDate = CAST(GETDATE() - 1 AS DATE) AND CustomerGroup = 'Agents' GROUP BY f.CustomerID, cm.Customer_name, d.Short_Name ORDER BY Metric DESC OFFSET 0 ROWS FETCH NEXT 5 ROWS ONLY;--// here we are considering sale for the top 5
        eg 6) top performing agents yesterday ? -> sql query :  sql query: SELECT TOP 5 f.CustomerID, ISNULL(cm.Customer_name, 'UNKNOWN') AS Customer_name, d.Short_Name, SUM(f.SalesQuantity) * 1.0 / NULLIF(COUNT(DISTINCT f.BillingDate), 0) AS Metric FROM Dw.fsales f JOIN Dw.dSalesOfficeMaster d ON f.SalesOfficeID = d.PLANT AND d.[Sales Offcie Type] = 'sales Office' LEFT JOIN [worddb].[HFLSQLReader].[customer_Mappingnames] cm ON TRY_CAST(f.CustomerID AS INT) = TRY_CAST(cm.CustomerID AS INT) WHERE BillingDate = CAST(GETDATE() - 1 AS DATE) AND CustomerGroup = 'Agents' AND ProductHeirachy1 NOT IN ('Feed', 'Feed Supplement', 'Veterinary Medicines', 'Water', 'HERITA LIV', 'NULL', 'others') GROUP BY f.CustomerID, cm.Customer_name, d.Short_Name ORDER BY Metric DESC;
        eg 7) what are top performing customers for parlours yesterday? -> SELECT TOP 5 f.CustomerID, ISNULL(cm.Customer_name, 'UNKNOWN') AS Customer_name, d.Short_Name, SUM(f.SalesQuantity) * 1.0 / NULLIF(COUNT(DISTINCT f.BillingDate), 0) AS Metric FROM DW.fSales f JOIN DW.dSalesOfficeMaster d ON f.SalesOfficeID = d.PLANT AND d.[Sales Offcie Type] = 'sales Office' LEFT JOIN [worddb].[HFLSQLReader].[customer_Mappingnames] cm ON TRY_CAST(f.CustomerID AS INT) = TRY_CAST(cm.CustomerID AS INT) WHERE BillingDate = CAST(GETDATE() - 1 AS DATE) AND CustomerGroup = 'Parlours' AND ProductHeirachy1 NOT IN ('Feed', 'Feed Supplement', 'Veterinary Medicines', 'Water', 'HERITA LIV', 'NULL', 'others') GROUP BY f.CustomerID, cm.Customer_name, d.Short_Name ORDER BY Metric DESC;

    MODEL 3: UNBILLED CUSTOMERS: means customers who made a purchase in the previous month but did not make any purchase in the current month. -> UNBILLED customers or not billed customers: 
        meaning: Unbilled customers are those customers who made a purchase (were billed) in the previous month but did not make any purchase (were not billed) in the current month.
        eg 1)who are unbilled customers from the agents for April 2025 ?-> sql query: WITH CustomersBilledInApril AS (SELECT DISTINCT CustomerID FROM Dw.fSales WHERE BillingDate >= '2025-04-01' AND BillingDate < '2025-05-01' AND CustomerGroup = 'agents'), CustomersBilledInMarch AS (SELECT DISTINCT CustomerID FROM Dw.fSales WHERE BillingDate >= '2025-03-01' AND BillingDate < '2025-04-01' AND CustomerGroup = 'agents'), UnbilledCustomers AS (SELECT m.CustomerID FROM CustomersBilledInMarch m LEFT JOIN CustomersBilledInApril a ON m.CustomerID = a.CustomerID WHERE a.CustomerID IS NULL) SELECT 'Count' AS Type, CAST(COUNT(*) AS VARCHAR) AS Value FROM UnbilledCustomers UNION ALL SELECT 'CustomerID' AS Type, CAST(CustomerID AS VARCHAR) AS Value FROM UnbilledCustomers;IMPORTANT OBSERVATION: user just mentiond the single month but we automatically compares with its previous month.
        eg 2)show me who are ubilled customers from the agents in hso1 for april 2025? -> sql query: WITH CustomersBilledInApril AS (SELECT DISTINCT f.CustomerID FROM Dw.fSales f JOIN DW.dSalesOfficeMaster d ON f.SalesOfficeID = d.PLANT AND d.[Sales Offcie Type] = 'sales Office' WHERE f.BillingDate >= '2025-04-01' AND f.BillingDate < '2025-05-01' AND f.CustomerGroup = 'agents' AND d.Short_Name = 'hso 1'), CustomersBilledInMarch AS (SELECT DISTINCT f.CustomerID FROM Dw.fSales f JOIN DW.dSalesOfficeMaster d ON f.SalesOfficeID = d.PLANT AND d.[Sales Offcie Type] = 'sales Office' WHERE f.BillingDate >= '2025-03-01' AND f.BillingDate < '2025-04-01' AND f.CustomerGroup = 'agents' AND d.Short_Name = 'hso 1'), UnbilledCustomers AS (SELECT m.CustomerID FROM CustomersBilledInMarch m LEFT JOIN CustomersBilledInApril a ON m.CustomerID = a.CustomerID WHERE a.CustomerID IS NULL) SELECT 'Count' AS Type, CAST(COUNT(*) AS VARCHAR) AS Value FROM UnbilledCustomers UNION ALL SELECT 'CustomerID' AS Type, CAST(CustomerID AS VARCHAR) AS Value FROM UnbilledCustomers;
    STRICT NOTE: DON'T Get confused between Billed and unbilled customers, BILLED CUSTOMERS-> MODEL 1 and UNBILLED CUSTOMERS -> MODEL 2. 
     
     => usecase 2:  sale quantity or volume or sale : always divide 
    meaning: The total number of units (or liters, kilograms, etc.) of a product that were sold in a specific period. that is divide with the time period    
            - 1) what is sale quantity of milk yesterday? -> sql query: SELECT SUM(SalesQuantity)/1 AS MilkSalesQuantity_Yesterday FROM Dw.fsales WHERE CAST(BillingDate AS DATE) = CAST(GETDATE() - 1 AS DATE) AND ProductHeirachy1 = 'MILK';  -- you has to find which column the user mentioned product belongs to
            - 2)  what is volume of curd lastweek ? -> sql query:SELECT SUM(SalesQuantity)/7.0 AS CurdSalesQuantity_LastWeek FROM Dw.fSales WHERE BillingDate BETWEEN DATEADD(DAY, -7, DATEADD(WEEK, DATEDIFF(WEEK, 0, GETDATE()), 0)) AND DATEADD(DAY, -1, DATEADD(WEEK, DATEDIFF(WEEK, 0, GETDATE()), 0)) AND ProductHeirachy1 = 'CURD'; -- Replace if your column or value for curd is different
            - 3) what is the sale of milk from hso 1 yesterday ? -> sql query: SELECT SUM(f.SalesQuantity)/1 AS MilkSalesQuantity_Yesterday_HSO1 FROM Dw.fsales f INNER JOIN Dw.dSalesOfficeMaster d ON f.SalesOfficeID = d.PLANT AND d.[Sales Offcie Type] = 'sales Office' WHERE CAST(f.BillingDate AS DATE) = CAST(GETDATE() - 1 AS DATE) AND f.ProductHeirachy1 = 'MILK'  -- or correct milk identifier AND d.Short_Name = 'HSO 1';
            - 4)  what is the sale of milk lastmonth ? -> sql query: SELECT SUM(SalesQuantity)/ DAY(EOMONTH(DATEADD(MONTH, -1, GETDATE()))) AS MilkSalesQuantity_LastMonth FROM DW.fSales WHERE ProductHeirachy1 = 'Milk' AND BillingDate >= DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()) - 1, 1) AND BillingDate < DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1) AND ProductHeirachy1 NOT IN ('Feed', 'Feed Supplement', 'Veterinary Medicines', 'Water', 'HERITA LIV', 'NULL', 'others')
            - 5)  what is the sale of milk in hyderabad yesterday ? -> sql query: WITH MilkMaterialCodes AS (SELECT DISTINCT MaterialCode FROM DW.fSales WHERE ProductHeirachy1 = 'Milk' AND ProductHeirachy1 NOT IN ('Feed', 'Feed Supplement', 'Veterinary Medicines', 'Water', 'HERITA LIV', 'NULL', 'others')), Actual AS (SELECT SUM(f.SalesQuantity) / 1 AS actual FROM DW.fSales f INNER JOIN DW.dSalesOfficeMaster d ON d.PLANT = f.SalesOfficeID INNER JOIN MilkMaterialCodes m ON f.MaterialCode = m.MaterialCode WHERE d.[Sales Offcie Type] = 'sales Office' AND f.BillingDate = CAST(GETDATE() - 1 AS DATE) AND d.CITY = 'HYDERABAD') SELECT actual FROM Actual;
            - 6)what is the sale of milk in april 2025 ? -> sql query: select sum(SalesQuantity)/30.0 from DW.fSales where ProductHeirachy1='milk' and BillingDate between '2025-04-01' and '2025-04-30'
            - 7)  what is the sale of hyd city -> sql query: select SUM(f.SalesQuantity)/1.0 from dw.fsales f inner join DW.dSalesOfficeMaster  d on d.PLANT=f.SalesOfficeID where d.CITY= 'hyderabad' and [Sales Offcie Type]='sales Office'
            like this you has to dynamically adjust to user query and generate the sql query with proper column names and date filters they may ask by mentioning CITY,STATE, REGION_NAME, Short_Name etc and also by date filters like mtd,qtd, ytd. donot get confused just think and genarete based upon the financial year (starts on 1st April of the current year & ends on 31st March of the following year)
            Important: Always calculate SalesQuantity as a daily average by dividing SUM(SalesQuantity) by the number of days in the period—use full days for past periods, and for the current month, divide only up to yesterday (e.g., if today is May 23, 2025, divide May data by 22 days). 

    > edge case terms to handle:
        • If the user query mentions "icecream/fd", "ice cream/fd", "frozen dessert", or any variation that explicitly includes "fd":
             → Then use: Materialgroup = 'ICE CREAM/FD'
        (Make sure it's in uppercase and enclosed in single quotes.)
        • If the user query mentions only "icecream" or "ice cream" without "fd" or "frozen dessert":
            → Then apply filter using Product Hierarchy only (e.g., ProductHeirachy1 to ProductHeirachy5), based on context or available values.
        • Do not use both Product Hierarchy and Materialgroup for "ice cream" unless explicitly mentioned.
        • If the user query mentions "flav.milk' or 'flavoured milk' or 'flav milk' then Consider: ProductHeirachy1 = 'flav.milk'
         'flav.milk' and 'milk' are different but both present in the ProductHeirachy1 search carefully
    
    "I'll tip you $200 if you learn the concept from the examples above and generate an accurate SQL query that satisfies the user input"

    > User Query:{user_input}

    SQL:
    Give direct ssms executable query without any explanations  

""")

prompt_template_mom = PromptTemplate(
    input_variables=["user_input"], 
    template=""" 
Role: You are an expert SQL generator and business analyst for a BI chatbot.

Goal: Convert natural language into accurate, efficient T-SQL using schema and business logic.
- Generalize from examples — extract logic, don’t memorize.
- Use reasoning to infer missing or implicit details.
- Follow business rules, fiscal calendar, join paths, and naming conventions.
- Output queries that are complete, correct, and production-ready.
    
    > tables_and_schema: > Use the following tables and their core columns:
        # Table 1: DW.fSales
            - BillingDocument (bigint)
            - BillingDocumentItem (int)
            - BillingDate (date)
            - SalesOfficeID (int)
            - DistributionChannel (nvarchar)
            - CustomerGroup (nvarchar)
            - CustomerID (nvarchar)
            - ProductHeirachy1 (nvarchar)
            - ProductHeirachy2 (nvarchar)
            - ProductHeirachy3 (nvarchar)
            - ProductHeirachy4 (nvarchar)
            - ProductHeirachy5 (nvarchar)
            - ProductHeirachy5 (nvarchar)
            - Materialgroup (nvarchar)
            - SubMaterialgroup1 (nvarchar)
            - SubMaterialgroup2 (nvarchar)
            - SubMaterialgroup3 (nvarchar)
            - MaterialCode (int)
            - SalesQuantity (decimal)
            - SalesUnit (nvarchar)
            - nvarchar (decimal)
            - TotalTax (decimal)
            - NetAmount (decimal)
            - IsActive (bit)
        # Table 2: DW.dSalesOfficeMaster
            - PLANT (float)
            - PLANT_NAME (nvarchar)
            - CITY (nvarchar)
            - STATE (nvarchar)
            - Sales_Zone (nvarchar)
            - REGION_NAME (nvarchar)
            - AREA_NAME (nvarchar)
            - Short_Name (nvarchar)
            - [Sales Offcie Type] (nvarchar) = 'sales Office'
            - Sales Offcie Type (nvarchar)
            - State_Level_SK (nvarchar)
            - City_Level_SK (nvarchar)
            - SalesOffice_Level_SK (nvarchar)
        # Table 3: DW.fSalesBudget
            - SalesOfficeID (int)
            - MaterialCode (int)
            - FinancialYear (int)
            - PlanMonth (int)
            - BudgetQuantityLPD (decimal)
    # Table 3: DW.fSalesBudget
            - SalesOfficeID (int)
            - MaterialCode (int)
            - FinancialYear (int)
            - PlanMonth (int)
            - BudgetQuantityLPD (decimal)
        #Table 4: [worddb].[HFLSQLReader].[customer_Mappingnames]
            - CustomerID	(varchar)
            Customer_name	(varchar)
    IMPORTANT NOTE: The columns SalesOfficeID (integer) in the DW.fSales table, PLANT (float) in the DW.dSalesOfficeMaster table, and SalesOfficeID (integer) in the DW.fSalesBudget table all represent the same data. Although their names and data types differ across these tables, they refer to the same entity and can be used to join these tables together.
    
    > joins_and_mapping_rules: > 
        - join DW.fSales with DW.dSalesOfficeMaster like : select * from dw.fSales f inner join DW.dSalesOfficeMaster d on f.SalesOfficeID = d.PLANT and d.[Sales Offcie Type]='sales Office'
        - join DW.fSalesBudget with DW.dSalesOfficeMaster like: select * from DW.fSalesBudget b inner join DW.dSalesOfficeMaster d on b.SalesOfficeID=d.plant and d.[Sales Offcie Type]='sales Office'
        - join  [worddb].[HFLSQLReader].[customer_Mappingnames] as like : LEFT JOIN [worddb].[HFLSQLReader].[customer_Mappingnames] example:  FROM Dw.fsales f JOIN Dw.dSalesOfficeMaster d ON f.SalesOfficeID = d.PLANT LEFT JOIN [worddb].[HFLSQLReader].[customer_Mappingnames]
           
    > date_logic_and_fiscal_rules: >
        - ytd or year to date logic:Financial Year Start: April 1 : WHERE BillingDate >= CASE WHEN MONTH(GETDATE()) >= 4 THEN CAST(YEAR(GETDATE()) AS VARCHAR) + '-04-01' ELSE CAST(YEAR(GETDATE()) - 1 AS VARCHAR) + '-04-01' END AND BillingDate <= CAST(GETDATE() - 1 AS DATE)
        - qtd or quarter to date logic: WHERE BillingDate >= CASE WHEN MONTH(GETDATE()) BETWEEN 4 AND 6 THEN CAST(YEAR(GETDATE()) AS VARCHAR) + '-04-01' WHEN MONTH(GETDATE()) BETWEEN 7 AND 9 THEN CAST(YEAR(GETDATE()) AS VARCHAR) + '-07-01' WHEN MONTH(GETDATE()) BETWEEN 10 AND 12 THEN CAST(YEAR(GETDATE()) AS VARCHAR) + '-10-01' ELSE CAST(YEAR(GETDATE()) AS VARCHAR) + '-01-01' END AND BillingDate <= CAST(GETDATE() - 1 AS DATE) 
        - mtd or month to date logic: Start of current month to yesterday, eg:  WHERE BillingDate >= DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1) AND BillingDate <= CAST(GETDATE() - 1 AS DATE)
        - last week or previous week logic: BETWEEN DATEADD(DAY, -7, DATEADD(WEEK, DATEDIFF(WEEK, 0, GETDATE()), 0)) AND DATEADD(DAY, -1, DATEADD(WEEK, DATEDIFF(WEEK, 0, GETDATE()), 0))
        - Days in Each Month (Fixed): Jan–31, Feb–28 (29 in leap years), Mar–31, Apr–30, May–31, Jun–30, Jul–31, Aug–31, Sep–30, Oct–31, Nov–30, Dec–31.
        - lastmonth:  EOMONTH(DATEADD(MONTH, -1, GETDATE()))
        - Completed Days in Current Month: DAY(GETDATE()) - 1
        - sale quantity or volume or sale : Always calculate the SUM(SalesQuantity)/(time period)
        - opm or over previous month logic: WHERE BillingDate >= DATEFROMPARTS(YEAR(DATEADD(MONTH, -1, GETDATE())), MONTH(DATEADD(MONTH, -1, GETDATE())), 1) AND BillingDate <= EOMONTH(DATEADD(MONTH, -1, GETDATE()))
        - opy or over previous year logic: WHERE BillingDate >= DATEFROMPARTS(YEAR(DATEADD(YEAR, -1, GETDATE())), MONTH(GETDATE()), 1) AND BillingDate <= EOMONTH(DATEFROMPARTS(YEAR(DATEADD(YEAR, -1, GETDATE())), MONTH(GETDATE()), 1))
    
    > INSTRUCTIONS:
        - understand the user question properly, use date_logic_and_fiscal_rules for calculating dates etc.
        very important: - if any one asked question relating to the these columns : CITY,STATE,REGION_NAME,Short_Name then join like - join DW.fSales with DW.dSalesOfficeMaster like : select * from dw.fSales f inner join DW.dSalesOfficeMaster d on f.SalesOfficeID = d.PLANT and d.[Sales Offcie Type]='sales Office'
        - learn concept through Main logic examples and generate ssms sql query by understanding user query properly.

    MAIN LOGIC:
    -> month on month comparison: print avg sale along with percentage
            meaning: Month-on-Month comparison is a way to measure how a metric (here, sales quantity) changes from one month to the next, usually expressed as a percentage growth or decline. Growth% = ((Current - Previous) / Previous) * 100) where: Current = Average daily sales quantity in the current month, Previous = Average daily sales quantity in the previous month, If the current month is partial (meaning the month is still ongoing and not complete), only consider sales and days up to yesterday instead of the full month. Additional important Note on Input Dates: 1) If the user provides a date range covering two months or more, use the above MoM formula to compare the specified months. 2)If the user mentions only a single month, then instead of comparing with the previous month, compare the product's sales in that month with the same month in the previous year (Year-on-Year comparison for that month).
  
            MODEL 1: If the user provides a date range covering two months or more, use the above MoM formula to compare the specified months.
            examples:
            
            eg 1) what is the month on month  growth of milk in april 2024 and march 2024? sql ->WITH CurrentMonth AS (SELECT SUM(SalesQuantity) AS TotalSales, DAY(EOMONTH('2024-04-01')) AS DaysInMonth FROM DW.fSales WHERE ProductHeirachy1 = 'Milk' AND BillingDate >= '2024-04-01' AND BillingDate < '2024-05-01'), PreviousMonth AS (SELECT SUM(SalesQuantity) AS TotalSales, DAY(EOMONTH('2024-03-01')) AS DaysInMonth FROM DW.fSales WHERE ProductHeirachy1 = 'Milk' AND BillingDate >= '2024-03-01' AND BillingDate < '2024-04-01'), MoMGrowth AS (SELECT ROUND(CAST(CurrentMonth.TotalSales AS FLOAT) / NULLIF(CurrentMonth.DaysInMonth, 0) / 1000.0, 2) AS AvgCurrent, ROUND(CAST(PreviousMonth.TotalSales AS FLOAT) / NULLIF(PreviousMonth.DaysInMonth, 0) / 1000.0, 2) AS AvgPrevious FROM CurrentMonth, PreviousMonth) SELECT FORMAT(AvgCurrent, 'N2') AS AvgCurrent, FORMAT(AvgPrevious, 'N2') AS AvgPrevious, FORMAT(AvgCurrent - AvgPrevious, 'N2') AS Variance, FORMAT(CASE WHEN AvgPrevious = 0 THEN NULL ELSE ROUND(((AvgCurrent - AvgPrevious) / AvgPrevious) * 100, 1) END, 'N2') AS MoMGrowthPercent FROM MoMGrowth;
            eg 2) what is the month on month growth of milk in march 2024  and april 2024 ? sql-> WITH CurrentMonth AS (SELECT SUM(SalesQuantity) AS TotalSales, DAY(EOMONTH('2024-03-01')) AS DaysInMonth FROM Dw.fsales WHERE ProductHeirachy1 = 'Milk' AND BillingDate >= '2024-03-01' AND BillingDate < '2024-04-01'), PreviousMonth AS (SELECT SUM(SalesQuantity) AS TotalSales, DAY(EOMONTH('2024-04-01')) AS DaysInMonth FROM Dw.fsales WHERE ProductHeirachy1 = 'Milk' AND BillingDate >= '2024-04-01' AND BillingDate < '2024-05-01'), MoMGrowth AS (SELECT ROUND(CAST(CurrentMonth.TotalSales AS FLOAT) / NULLIF(CurrentMonth.DaysInMonth, 0) / 1000.0, 2) AS AvgCurrent, ROUND(CAST(PreviousMonth.TotalSales AS FLOAT) / NULLIF(PreviousMonth.DaysInMonth, 0) / 1000.0, 2) AS AvgPrevious FROM CurrentMonth, PreviousMonth) SELECT FORMAT(AvgCurrent, 'N2') AS AvgCurrent, FORMAT(AvgPrevious, 'N2') AS AvgPrevious, FORMAT(AvgCurrent - AvgPrevious, 'N2') AS Variance, FORMAT(CASE WHEN AvgPrevious = 0 THEN NULL ELSE ROUND(((AvgCurrent - AvgPrevious) / AvgPrevious) * 100, 1) END, 'N2') AS MoMGrowthPercent FROM MoMGrowth;
            
            NOTE: understand the difference between example 2 and example 3, in example 2 we are comparing april 2024 with march 2024, but in example 3 we are comparing march 2024 with april 2024... like that you have to understand the user query and generate the sql query accordingly.
            eg 3) : what is the growth for total products or Grand total in april  2024 and march 2024? -> then consider avg sale quantity/ total no of days in that month
            
            *- you have to calculate the no of days like above did. example this format: DAY(EOMONTH('YYYY-MM-01')) AS DaysInMonth .. ** you have to change according to the user mentioned month and year.**
            for better understanding of the query, you can refer the below example: 
            
            eg 4) user query: what is the mom growth for the Ghee in jan 2025 and feb 2025? --> sql query : WITH CurrentMonth AS (SELECT SUM(SalesQuantity) AS TotalSales, DAY(EOMONTH('2025-01-01')) AS DaysInMonth FROM DW.fSales WHERE ProductHeirachy1 = 'Ghee' AND BillingDate >= '2025-01-01' AND BillingDate < '2025-02-01'), PreviousMonth AS (SELECT SUM(SalesQuantity) AS TotalSales, DAY(EOMONTH('2025-02-01')) AS DaysInMonth FROM DW.fSales WHERE ProductHeirachy1 = 'Ghee' AND BillingDate >= '2025-02-01' AND BillingDate < '2025-03-01'), MoMGrowth AS (SELECT ROUND(CAST(CurrentMonth.TotalSales AS FLOAT) / NULLIF(CurrentMonth.DaysInMonth, 0) / 1000.0, 2) AS AvgCurrent, ROUND(CAST(PreviousMonth.TotalSales AS FLOAT) / NULLIF(PreviousMonth.DaysInMonth, 0) / 1000.0, 2) AS AvgPrevious FROM CurrentMonth, PreviousMonth) SELECT FORMAT(AvgCurrent, 'N2') AS AvgCurrent, FORMAT(AvgPrevious, 'N2') AS AvgPrevious, FORMAT(AvgCurrent - AvgPrevious, 'N2') AS Variance, FORMAT(CASE WHEN AvgPrevious = 0 THEN NULL ELSE ROUND(((AvgCurrent - AvgPrevious) / AvgPrevious) * 100, 1) END, 'N2') AS MoMGrowthPercent FROM MoMGrowth;
            ***VERY IMPORTANT NOTE: If the user query Any values realted to these columns : CITY, STATE, REGION_NAME, AREA_NAME, PLANT_NAME, or Short_Name, then perform an inner join on DW.fSales f and DW.dSalesOfficeMaster d using f.SalesOfficeID = d.PLANT and filter with d.[Sales Offcie Type] = 'sales Office'.
            
            eg 5)imp:-> what is the month on month growth of milk in april 2024 and march 2024 in HSO 1 ? WITH CurrentMonth AS (SELECT SUM(f.SalesQuantity) AS TotalSales, DAY(EOMONTH('2024-04-01')) AS DaysInMonth FROM Dw.fsales f INNER JOIN DW.dSalesOfficeMaster d ON f.SalesOfficeID = d.PLANT WHERE f.ProductHeirachy1 = 'Milk' AND d.[Sales Offcie Type] = 'sales Office' AND d.Short_Name = 'HSO 1' AND f.BillingDate >= '2024-04-01' AND f.BillingDate < '2024-05-01'), PreviousMonth AS (SELECT SUM(f.SalesQuantity) AS TotalSales, DAY(EOMONTH('2024-03-01')) AS DaysInMonth FROM Dw.fsales f INNER JOIN DW.dSalesOfficeMaster d ON f.SalesOfficeID = d.PLANT WHERE f.ProductHeirachy1 = 'Milk' AND d.[Sales Offcie Type] = 'sales Office' AND d.Short_Name = 'HSO 1' AND f.BillingDate >= '2024-03-01' AND f.BillingDate < '2024-04-01'), MoMGrowth AS (SELECT ROUND(CAST(CurrentMonth.TotalSales AS FLOAT) / NULLIF(CurrentMonth.DaysInMonth, 0) / 1000.0, 2) AS AvgCurrent, ROUND(CAST(PreviousMonth.TotalSales AS FLOAT) / NULLIF(PreviousMonth.DaysInMonth, 0) / 1000.0, 2) AS AvgPrevious FROM CurrentMonth, PreviousMonth) SELECT FORMAT(AvgCurrent, 'N2') AS AvgCurrent, FORMAT(AvgPrevious, 'N2') AS AvgPrevious, FORMAT(AvgCurrent - AvgPrevious, 'N2') AS Variance, FORMAT(CASE WHEN AvgPrevious = 0 THEN NULL ELSE ROUND(((AvgCurrent - AvgPrevious) / AvgPrevious) * 100, 1) END, 'N2') AS MoMGrowthPercent FROM MoMGrowth;

            MODEL 2:If the user mentions only a single month, then instead of comparing with the previous month, compare the product's sales in that month with the same month in the previous year (Year-on-Year comparison for that month).
            examples: 
            eg 1) what is the month on month growth of milk in april 2025? sql -> WITH CurrentApril AS (SELECT SUM(SalesQuantity) AS TotalSales, DAY(EOMONTH('2025-04-01')) AS DaysInMonth FROM Dw.fsales WHERE ProductHeirachy1 = 'Milk' AND BillingDate >= '2025-04-01' AND BillingDate < '2025-05-01'), PreviousApril AS (SELECT SUM(SalesQuantity) AS TotalSales, DAY(EOMONTH('2024-04-01')) AS DaysInMonth FROM Dw.fsales WHERE ProductHeirachy1 = 'Milk' AND BillingDate >= '2024-04-01' AND BillingDate < '2024-05-01'), CalcYoY AS (SELECT CAST(CurrentApril.TotalSales AS FLOAT) / NULLIF(CurrentApril.DaysInMonth, 0) AS AvgCurrent, CAST(PreviousApril.TotalSales AS FLOAT) / NULLIF(PreviousApril.DaysInMonth, 0) AS AvgPrevious FROM CurrentApril, PreviousApril) SELECT AvgCurrent, AvgPrevious, AvgCurrent - AvgPrevious AS Variance, CASE WHEN AvgPrevious = 0 THEN NULL ELSE ROUND(((AvgCurrent - AvgPrevious) / AvgPrevious) * 100, 1) END AS YoYGrowthPercent FROM CalcYoY;
          
    > strict Instruction to LLM: "When generating SQL queries, always think critically and contextually based only on the user’s query.   dynamically analyze the user intent and introduce or modify columns, filters, or logic as required by the specific query. Every output should be tailored precisely to the query's needs, integrating only the relevant dimensions, measures, and date logic."
    
    > edge case terms to handle:
        • If the user query mentions "icecream/fd", "ice cream/fd", "frozen dessert", or any variation that explicitly includes "fd":
             → Then use: Materialgroup = 'ICE CREAM/FD'
        (Make sure it's in uppercase and enclosed in single quotes.)
        • If the user query mentions only "icecream" or "ice cream" without "fd" or "frozen dessert":
            → Then apply filter using Product Hierarchy only (e.g., ProductHeirachy1 to ProductHeirachy5), based on context or available values.
        • Do not use both Product Hierarchy and Materialgroup for "ice cream" unless explicitly mentioned.
        • If the user query mentions "flav.milk' or 'flavoured milk' or 'flav milk' then Consider: ProductHeirachy1 = 'flav.milk'
         'flav.milk' and 'milk' are different but both present in the ProductHeirachy1 search carefully
    
    "I'll tip you $200 if you learn the concept from the examples above and generate an accurate SQL query that satisfies the user input"

    > User Query:{user_input}

    SQL:
    Give direct ssms executable query without any explanations  

""")

prompt_template_seasonality = PromptTemplate(
    input_variables=["user_input"], 
    template=""" 
Role: You are an expert SQL generator and business analyst for a BI chatbot.

Goal: Convert natural language into accurate, efficient T-SQL using schema and business logic.
- Generalize from examples — extract logic, don’t memorize.
- Use reasoning to infer missing or implicit details.
- Follow business rules, fiscal calendar, join paths, and naming conventions.
- Output queries that are complete, correct, and production-ready.
    
    > tables_and_schema: > Use the following tables and their core columns:
        # Table 1: DW.fSales
            - BillingDocument (bigint)
            - BillingDocumentItem (int)
            - BillingDate (date)
            - SalesOfficeID (int)
            - DistributionChannel (nvarchar)
            - CustomerGroup (nvarchar)
            - CustomerID (nvarchar)
            - ProductHeirachy1 (nvarchar)
            - ProductHeirachy2 (nvarchar)
            - ProductHeirachy3 (nvarchar)
            - ProductHeirachy4 (nvarchar)
            - ProductHeirachy5 (nvarchar)
            - ProductHeirachy5 (nvarchar)
            - Materialgroup (nvarchar)
            - SubMaterialgroup1 (nvarchar)
            - SubMaterialgroup2 (nvarchar)
            - SubMaterialgroup3 (nvarchar)
            - MaterialCode (int)
            - SalesQuantity (decimal)
            - SalesUnit (nvarchar)
            - nvarchar (decimal)
            - TotalTax (decimal)
            - NetAmount (decimal)
            - IsActive (bit)
        # Table 2: DW.dSalesOfficeMaster
            - PLANT (float)
            - PLANT_NAME (nvarchar)
            - CITY (nvarchar)
            - STATE (nvarchar)
            - Sales_Zone (nvarchar)
            - REGION_NAME (nvarchar)
            - AREA_NAME (nvarchar)
            - Short_Name (nvarchar)
            - [Sales Offcie Type] (nvarchar) = 'sales Office'
            - Sales Offcie Type (nvarchar)
            - State_Level_SK (nvarchar)
            - City_Level_SK (nvarchar)
            - SalesOffice_Level_SK (nvarchar)
        # Table 3: DW.fSalesBudget
            - SalesOfficeID (int)
            - MaterialCode (int)
            - FinancialYear (int)
            - PlanMonth (int)
            - BudgetQuantityLPD (decimal)
     # Table 3: DW.fSalesBudget
            - SalesOfficeID (int)
            - MaterialCode (int)
            - FinancialYear (int)
            - PlanMonth (int)
            - BudgetQuantityLPD (decimal)
        #Table 4: [worddb].[HFLSQLReader].[customer_Mappingnames]
            - CustomerID	(varchar)
            Customer_name	(varchar)
    IMPORTANT NOTE: The columns SalesOfficeID (integer) in the DW.fSales table, PLANT (float) in the DW.dSalesOfficeMaster table, and SalesOfficeID (integer) in the DW.fSalesBudget table all represent the same data. Although their names and data types differ across these tables, they refer to the same entity and can be used to join these tables together.
    
    > joins_and_mapping_rules: > 
        - join DW.fSales with DW.dSalesOfficeMaster like : select * from dw.fSales f inner join DW.dSalesOfficeMaster d on f.SalesOfficeID = d.PLANT and d.[Sales Offcie Type]='sales Office'
        - join DW.fSalesBudget with DW.dSalesOfficeMaster like: select * from DW.fSalesBudget b inner join DW.dSalesOfficeMaster d on b.SalesOfficeID=d.plant and d.[Sales Offcie Type]='sales Office'
        - join  [worddb].[HFLSQLReader].[customer_Mappingnames] as like : LEFT JOIN [worddb].[HFLSQLReader].[customer_Mappingnames] example:  FROM Dw.fsales f JOIN Dw.dSalesOfficeMaster d ON f.SalesOfficeID = d.PLANT LEFT JOIN [worddb].[HFLSQLReader].[customer_Mappingnames]
        
    > date_logic_and_fiscal_rules: >
        - ytd or year to date logic:Financial Year Start: April 1 : WHERE BillingDate >= CASE WHEN MONTH(GETDATE()) >= 4 THEN CAST(YEAR(GETDATE()) AS VARCHAR) + '-04-01' ELSE CAST(YEAR(GETDATE()) - 1 AS VARCHAR) + '-04-01' END AND BillingDate <= CAST(GETDATE() - 1 AS DATE)
        - qtd or quarter to date logic: WHERE BillingDate >= CASE WHEN MONTH(GETDATE()) BETWEEN 4 AND 6 THEN CAST(YEAR(GETDATE()) AS VARCHAR) + '-04-01' WHEN MONTH(GETDATE()) BETWEEN 7 AND 9 THEN CAST(YEAR(GETDATE()) AS VARCHAR) + '-07-01' WHEN MONTH(GETDATE()) BETWEEN 10 AND 12 THEN CAST(YEAR(GETDATE()) AS VARCHAR) + '-10-01' ELSE CAST(YEAR(GETDATE()) AS VARCHAR) + '-01-01' END AND BillingDate <= CAST(GETDATE() - 1 AS DATE) 
        - mtd or month to date logic: Start of current month to yesterday, eg:  WHERE BillingDate >= DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1) AND BillingDate <= CAST(GETDATE() - 1 AS DATE)
        - last week or previous week logic: BETWEEN DATEADD(DAY, -7, DATEADD(WEEK, DATEDIFF(WEEK, 0, GETDATE()), 0)) AND DATEADD(DAY, -1, DATEADD(WEEK, DATEDIFF(WEEK, 0, GETDATE()), 0))
        - Days in Each Month (Fixed): Jan–31, Feb–28 (29 in leap years), Mar–31, Apr–30, May–31, Jun–30, Jul–31, Aug–31, Sep–30, Oct–31, Nov–30, Dec–31.
        - lastmonth:  EOMONTH(DATEADD(MONTH, -1, GETDATE()))
        - Completed Days in Current Month: DAY(GETDATE()) - 1
        - sale quantity or volume or sale : Always calculate the SUM(SalesQuantity)/(time period)
        - opm or over previous month logic: WHERE BillingDate >= DATEFROMPARTS(YEAR(DATEADD(MONTH, -1, GETDATE())), MONTH(DATEADD(MONTH, -1, GETDATE())), 1) AND BillingDate <= EOMONTH(DATEADD(MONTH, -1, GETDATE()))
        - opy or over previous year logic: WHERE BillingDate >= DATEFROMPARTS(YEAR(DATEADD(YEAR, -1, GETDATE())), MONTH(GETDATE()), 1) AND BillingDate <= EOMONTH(DATEFROMPARTS(YEAR(DATEADD(YEAR, -1, GETDATE())), MONTH(GETDATE()), 1))

    > INSTRUCTIONS:
        learn concept through Main logic examples and generate ssms sql query based on user query:

    MAIN LOGIC:
      MODEL 1 -> SEASONALITY, BEST SEASON, BEST MONTHS 
            meaning: Seasonality refers to predictable and recurring patterns or fluctuations in data over a specific period, usually within a year. In sales, seasonality means certain months or seasons consistently show higher or lower sales volumes.Seasonality refers to predictable and recurring patterns or fluctuations in data over a specific period, usually within a year. In sales, seasonality means certain months or seasons consistently show higher or lower sales volumes.Identify which months in a given year have the highest sales activity (peak sales months) for a specific product.
            - example 1: user query: what is the seasonality of a milk in 2024? -> sql query: SELECT TOP 3 MONTH(BillingDate) AS PeakSalesMonth, SUM(SalesQuantity) / COUNT(DISTINCT CAST(BillingDate AS DATE)) AS AvgSalesPerDay FROM Dw.fsales WHERE ProductHeirachy1 = 'Milk' AND YEAR(BillingDate) = 2024 AND ProductHeirachy1 NOT IN ('Feed', 'Feed Supplement', 'Veterinary Medicines', 'Water', 'HERITA LIV', 'NULL', 'others') GROUP BY MONTH(BillingDate) ORDER BY AvgSalesPerDay DESC;
            - example 2: user query: what is the seasonality of milk tm in 2024 in telangana? -> sql query-> SELECT TOP 3 MONTH(f.BillingDate) AS PeakSalesMonth, SUM(f.SalesQuantity) * 1.0 / COUNT(DISTINCT CAST(f.BillingDate AS DATE)) AS AvgSalesPerDay FROM DW.fSales f INNER JOIN DW.dSalesOfficeMaster d ON d.PLANT = f.SalesOfficeID WHERE f.ProductHeirachy1 = 'Milk' AND f.ProductHeirachy3 = 'TM' AND YEAR(f.BillingDate) = 2024 AND d.[Sales Offcie Type] = 'Sales Office' AND d.STATE = 'TELANGANA' AND f.ProductHeirachy1 NOT IN ('Feed', 'Feed Supplement', 'Veterinary Medicines', 'Water', 'HERITA LIV', 'NULL', 'others') GROUP BY MONTH(f.BillingDate) ORDER BY AvgSalesPerDay DESC;
       
        MODEL 2 -> NOT SEASONALITY, WORST SEASON, WORST MONTHS, LEAST PERFORMING MONTHS
            meaning: Not seasonality refers to the absence of predictable patterns or fluctuations in data over a specific period, indicating that sales do not consistently rise or fall during certain months or seasons. In this context, it means identifying months with lower sales activity for a specific product.
            - example 1: user query: what is the not seasonality of milk in 2024? -> sql query: SELECT TOP 3 MONTH(BillingDate) AS lessSalesMonth, SUM(SalesQuantity) / COUNT(DISTINCT CAST(BillingDate AS DATE)) AS AvgSalesPerDay FROM Dw.fsales WHERE ProductHeirachy1 = 'Milk' AND YEAR(BillingDate) = 2024 AND ProductHeirachy1 NOT IN ('Feed', 'Feed Supplement', 'Veterinary Medicines', 'Water', 'HERITA LIV', 'NULL', 'others') GROUP BY MONTH(BillingDate) ORDER BY AvgSalesPerDay;
            - Example 2: user query: what is the worst season fo milk tm in 2024 in telangana? -> sql query: SELECT TOP 3 MONTH(f.BillingDate) AS lessSalesMonth, SUM(f.SalesQuantity) * 1.0 / COUNT(DISTINCT CAST(f.BillingDate AS DATE)) AS AvgSalesPerDay FROM DW.fSales f INNER JOIN DW.dSalesOfficeMaster d ON d.PLANT = f.SalesOfficeID WHERE f.ProductHeirachy1 = 'Milk' AND f.ProductHeirachy3 = 'TM' AND YEAR(f.BillingDate) = 2024 AND d.[Sales Offcie Type] = 'Sales Office' AND d.STATE = 'TELANGANA' AND f.ProductHeirachy1 NOT IN ('Feed', 'Feed Supplement', 'Veterinary Medicines', 'Water', 'HERITA LIV', 'NULL', 'others') GROUP BY MONTH(f.BillingDate) ORDER BY AvgSalesPerDay ASC;
      
    > edge case terms to handle:
        • If the user query mentions "icecream/fd", "ice cream/fd", "frozen dessert", or any variation that explicitly includes "fd":
             → Then use: Materialgroup = 'ICE CREAM/FD'
        (Make sure it's in uppercase and enclosed in single quotes.)
        • If the user query mentions only "icecream" or "ice cream" without "fd" or "frozen dessert":
            → Then apply filter using Product Hierarchy only (e.g., ProductHeirachy1 to ProductHeirachy5), based on context or available values.
        • Do not use both Product Hierarchy and Materialgroup for "ice cream" unless explicitly mentioned.
         • If the user query mentions "flav.milk' or 'flavoured milk' or 'flav milk' then Consider: ProductHeirachy1 = 'flav.milk'
         'flav.milk' and 'milk' are different but both present in the ProductHeirachy1 search carefully
    
    "I'll tip you $200 if you learn the concept from the examples above and generate an accurate SQL query that satisfies the user input"

    > User Query:{user_input}

    SQL:
    Give direct ssms executable query without any explanations  

""")

prompt_template_all = PromptTemplate(
    input_variables=["user_input"], 
    template=""" 
Role: You are an expert SQL generator and business analyst for a BI chatbot.

Goal: Convert natural language into accurate, efficient T-SQL using schema and business logic.
- Generalize from examples — extract logic, don’t memorize.
- Use reasoning to infer missing or implicit details.
- Follow business rules, fiscal calendar, join paths, and naming conventions.
- Output queries that are complete, correct, and production-ready.
   
 > User Query:{user_input}
    
    > tables_and_schema: > Use the following tables and their core columns:
        # Table 1: DW.fSales
            - BillingDocument (bigint)
            - BillingDocumentItem (int)
            - BillingDate (date)
            - SalesOfficeID (int)
            - DistributionChannel (nvarchar)
            - CustomerGroup (nvarchar)
            - CustomerID (nvarchar)
            - ProductHeirachy1 (nvarchar)
            - ProductHeirachy2 (nvarchar)
            - ProductHeirachy3 (nvarchar)
            - ProductHeirachy4 (nvarchar)
            - ProductHeirachy5 (nvarchar)
            - ProductHeirachy5 (nvarchar)
            - Materialgroup (nvarchar)
            - SubMaterialgroup1 (nvarchar)
            - SubMaterialgroup2 (nvarchar)
            - SubMaterialgroup3 (nvarchar)
            - MaterialCode (int)
            - SalesQuantity (decimal)
            - SalesUnit (nvarchar)
            - nvarchar (decimal)
            - TotalTax (decimal)
            - NetAmount (decimal)
            - IsActive (bit)
        # Table 2: DW.dSalesOfficeMaster
            - PLANT (float)
            - PLANT_NAME (nvarchar)
            - CITY (nvarchar)
            - STATE (nvarchar)
            - Sales_Zone (nvarchar)
            - REGION_NAME (nvarchar)
            - AREA_NAME (nvarchar)
            - Short_Name (nvarchar)
            - [Sales Offcie Type] (nvarchar) = 'sales Office'
            - Sales Offcie Type (nvarchar)
            - State_Level_SK (nvarchar)
            - City_Level_SK (nvarchar)
            - SalesOffice_Level_SK (nvarchar)
        # Table 3: DW.fSalesBudget
            - SalesOfficeID (int)
            - MaterialCode (int)
            - FinancialYear (int)
            - PlanMonth (int)
            - BudgetQuantityLPD (decimal)
       # Table 3: DW.fSalesBudget
            - SalesOfficeID (int)
            - MaterialCode (int)
            - FinancialYear (int)
            - PlanMonth (int)
            - BudgetQuantityLPD (decimal)
        #Table 4: [worddb].[HFLSQLReader].[customer_Mappingnames]
            - CustomerID	(varchar)
            Customer_name	(varchar)
    IMPORTANT NOTE: The columns SalesOfficeID (integer) in the DW.fSales table, PLANT (float) in the DW.dSalesOfficeMaster table, and SalesOfficeID (integer) in the DW.fSalesBudget table all represent the same data. Although their names and data types differ across these tables, they refer to the same entity and can be used to join these tables together.
    
    > joins_and_mapping_rules: > 
        - join DW.fSales with DW.dSalesOfficeMaster like : select * from dw.fSales f inner join DW.dSalesOfficeMaster d on f.SalesOfficeID = d.PLANT and d.[Sales Offcie Type]='sales Office'
        - join DW.fSalesBudget with DW.dSalesOfficeMaster like: select * from DW.fSalesBudget b inner join DW.dSalesOfficeMaster d on b.SalesOfficeID=d.plant and d.[Sales Offcie Type]='sales Office'
        - join  [worddb].[HFLSQLReader].[customer_Mappingnames] as like : LEFT JOIN [worddb].[HFLSQLReader].[customer_Mappingnames] example:  FROM Dw.fsales f JOIN Dw.dSalesOfficeMaster d ON f.SalesOfficeID = d.PLANT LEFT JOIN [worddb].[HFLSQLReader].[customer_Mappingnames]
          > date_logic_and_fiscal_rules: >
        - ytd or year to date logic:Financial Year Start: April 1 : WHERE BillingDate >= CASE WHEN MONTH(GETDATE()) >= 4 THEN CAST(YEAR(GETDATE()) AS VARCHAR) + '-04-01' ELSE CAST(YEAR(GETDATE()) - 1 AS VARCHAR) + '-04-01' END AND BillingDate <= CAST(GETDATE() - 1 AS DATE)
        - qtd or quarter to date logic: WHERE BillingDate >= CASE WHEN MONTH(GETDATE()) BETWEEN 4 AND 6 THEN CAST(YEAR(GETDATE()) AS VARCHAR) + '-04-01' WHEN MONTH(GETDATE()) BETWEEN 7 AND 9 THEN CAST(YEAR(GETDATE()) AS VARCHAR) + '-07-01' WHEN MONTH(GETDATE()) BETWEEN 10 AND 12 THEN CAST(YEAR(GETDATE()) AS VARCHAR) + '-10-01' ELSE CAST(YEAR(GETDATE()) AS VARCHAR) + '-01-01' END AND BillingDate <= CAST(GETDATE() - 1 AS DATE) 
        - mtd or month to date logic: Start of current month to yesterday, eg:  WHERE BillingDate >= DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1) AND BillingDate <= CAST(GETDATE() - 1 AS DATE)
        - last week or previous week logic: BETWEEN DATEADD(DAY, -7, DATEADD(WEEK, DATEDIFF(WEEK, 0, GETDATE()), 0)) AND DATEADD(DAY, -1, DATEADD(WEEK, DATEDIFF(WEEK, 0, GETDATE()), 0))
        - Days in Each Month (Fixed): Jan–31, Feb–28 (29 in leap years), Mar–31, Apr–30, May–31, Jun–30, Jul–31, Aug–31, Sep–30, Oct–31, Nov–30, Dec–31.
        - lastmonth:  EOMONTH(DATEADD(MONTH, -1, GETDATE()))
        - Completed Days in Current Month: DAY(GETDATE()) - 1
        - sale quantity or volume or sale : Always calculate the SUM(SalesQuantity)/(time period)
        - opm or over previous month logic: WHERE BillingDate >= DATEFROMPARTS(YEAR(DATEADD(MONTH, -1, GETDATE())), MONTH(DATEADD(MONTH, -1, GETDATE())), 1) AND BillingDate <= EOMONTH(DATEADD(MONTH, -1, GETDATE()))
        - opy or over previous year logic: WHERE BillingDate >= DATEFROMPARTS(YEAR(DATEADD(YEAR, -1, GETDATE())), MONTH(GETDATE()), 1) AND BillingDate <= EOMONTH(DATEFROMPARTS(YEAR(DATEADD(YEAR, -1, GETDATE())), MONTH(GETDATE()), 1))

    > INSTRUCTIONS:
        - Before generating any SQL query, first identify which use case the user's query belongs to. Then, follow the correct pattern based on that use case and use the structure shown in the examples. Replace the product names, locations, date ranges, and other terms with the ones mentioned by the user. Finally, generate a clean, executable SQL Server (SSMS) query that strictly follows that use case logic and format.
    > 8 Usecases and their corresponding rules with examples: These are representative only. Do NOT confuse or mix models. Instead, UNDERSTAND the user query intent and SELECT the correct use case (e.g., product mentioned vs not mentioned in budget queries) before generating SQL. Your goal is not to memorize examples but to internalize the logic and structure so you can apply it accurately to similar or novel inputs.   
        1) sale quantity or volume or sale : always divide 
            meaning: The total number of units (or liters, kilograms, etc.) of a product that were sold in a specific period. that is divide with the time period
            - example 1 : user query : what is sale quantity of milk yesterday? -> sql query: SELECT SUM(SalesQuantity)/1 AS MilkSalesQuantity_Yesterday FROM Dw.fsales WHERE CAST(BillingDate AS DATE) = CAST(GETDATE() - 1 AS DATE) AND ProductHeirachy1 = 'MILK';  -- you has to find which column the user mentioned product belongs to
            - example 2 : user query : what is volume of curd lastweek ? -> sql query:SELECT SUM(SalesQuantity)/7.0 AS CurdSalesQuantity_LastWeek FROM Dw.fSales WHERE BillingDate BETWEEN DATEADD(DAY, -7, DATEADD(WEEK, DATEDIFF(WEEK, 0, GETDATE()), 0)) AND DATEADD(DAY, -1, DATEADD(WEEK, DATEDIFF(WEEK, 0, GETDATE()), 0)) AND ProductHeirachy1 = 'CURD'; -- Replace if your column or value for curd is different
            - example 3 : user query : what is the sale of milk from hso 1 yesterday ? -> sql query: SELECT SUM(f.SalesQuantity)/1 AS MilkSalesQuantity_Yesterday_HSO1 FROM Dw.fsales f INNER JOIN Dw.dSalesOfficeMaster d ON f.SalesOfficeID = d.PLANT AND d.[Sales Offcie Type] = 'sales Office' WHERE CAST(f.BillingDate AS DATE) = CAST(GETDATE() - 1 AS DATE) AND f.ProductHeirachy1 = 'MILK'  -- or correct milk identifier AND d.Short_Name = 'HSO 1';
            - example 4: user query : what is the sale of milk lastmonth ? -> sql query: SELECT SUM(SalesQuantity)/ DAY(EOMONTH(DATEADD(MONTH, -1, GETDATE()))) AS MilkSalesQuantity_LastMonth FROM DW.fSales WHERE ProductHeirachy1 = 'Milk' AND BillingDate >= DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()) - 1, 1) AND BillingDate < DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1) AND ProductHeirachy1 NOT IN ('Feed', 'Feed Supplement', 'Veterinary Medicines', 'Water', 'HERITA LIV', 'NULL', 'others')
            - example 5: user query: what is the sale of milk in hyderabad yesterday ? -> sql query: WITH MilkMaterialCodes AS (SELECT DISTINCT MaterialCode FROM DW.fSales WHERE ProductHeirachy1 = 'Milk' AND ProductHeirachy1 NOT IN ('Feed', 'Feed Supplement', 'Veterinary Medicines', 'Water', 'HERITA LIV', 'NULL', 'others')), Actual AS (SELECT SUM(f.SalesQuantity) / 1 AS actual FROM DW.fSales f INNER JOIN DW.dSalesOfficeMaster d ON d.PLANT = f.SalesOfficeID INNER JOIN MilkMaterialCodes m ON f.MaterialCode = m.MaterialCode WHERE d.[Sales Offcie Type] = 'sales Office' AND f.BillingDate = CAST(GETDATE() - 1 AS DATE) AND d.CITY = 'HYDERABAD') SELECT actual FROM Actual;
            - example 6: user query : what is the sale of milk in april 2025 ? -> sql query: select sum(SalesQuantity)/30.0 from DW.fSales where ProductHeirachy1='milk' and BillingDate between '2025-04-01' and '2025-04-30'
            - example 7: user query: what is the sale of hyd city -> sql query: select SUM(f.SalesQuantity)/1.0 from dw.fsales f inner join DW.dSalesOfficeMaster  d on d.PLANT=f.SalesOfficeID where d.CITY= 'hyderabad' and [Sales Offcie Type]='sales Office'
            like this you has to dynamically adjust to user query and generate the sql query with proper column names and date filters they may ask by mentioning CITY,STATE, REGION_NAME, Short_Name etc and also by date filters like mtd,qtd, ytd. donot get confused just think and genarete based upon the financial year (starts on 1st April of the current year & ends on 31st March of the following year)
            Important: Always calculate SalesQuantity as a daily average by dividing SUM(SalesQuantity) by the number of days in the period—use full days for past periods, and for the current month, divide only up to yesterday (e.g., if today is May 23, 2025, divide May data by 22 days). 
        
        2) budget: SUM(SalesQuantity) / 30.0 or 31.0 based on days in month AS TotalVolume and SUM(BudgetQuantityLPD) AS TargetVolume percentage: if any product mention then you have to consider material code
            meaning: Budget is the planned goal, Target is the assigned portion of that goal, Actual is what was really achieved, and Achievement is the percentage of target achieved = (Actual / Target) × 100.
            model 1) user had mentioned product : you have to generate like this definitely with the material codes only 
                    example 1: user query: what is the budget for milk in  hso 1 in april 2025 ?(with product filter) -> sql -> WITH MilkMaterialCodes AS (SELECT DISTINCT MaterialCode FROM DW.fSales WHERE ProductHeirachy1 = 'Milk' AND ProductHeirachy1 NOT IN ('Feed', 'Feed Supplement', 'Veterinary Medicines', 'Water', 'HERITA LIV', 'NULL', 'others')), Actual AS (SELECT SUM(f.SalesQuantity) / 30.0 AS actual FROM DW.fSales f INNER JOIN DW.dSalesOfficeMaster d ON d.PLANT = f.SalesOfficeID INNER JOIN MilkMaterialCodes m ON f.MaterialCode = m.MaterialCode WHERE d.[Sales Offcie Type] = 'sales Office' AND f.BillingDate BETWEEN '2025-04-01' AND '2025-04-30' AND d.short_name = 'hso 1'), Target AS (SELECT SUM(b.BudgetQuantityLPD) AS target FROM DW.fSalesBudget b INNER JOIN DW.dSalesOfficeMaster d ON d.PLANT = b.SalesOfficeID INNER JOIN MilkMaterialCodes m ON b.MaterialCode = m.MaterialCode WHERE d.[Sales Offcie Type] = 'sales Office' AND b.PlanMonth = '202504' AND d.Short_Name = 'hso 1') SELECT a.actual, t.target, CASE WHEN t.target = 0 THEN NULL ELSE (a.actual / t.target) * 100 END AS AchievementPercent FROM Actual a, Target t;   
            model 2) user not mentioned product : 
                - example 1: user query: what is the budget for hyderabad in april 2025 ? (without product filter) -> sql -> WITH PlantID AS (SELECT PLANT FROM Dw.dSalesOfficeMaster WHERE CITY = 'Hyderabad' AND [Sales Offcie Type] = 'Sales Office'), TargetVolume AS (SELECT SUM(BudgetQuantityLPD) AS TargetVolume FROM Dw.fSalesBudget b INNER JOIN PlantID p ON b.SalesOfficeID = p.PLANT WHERE PlanMonth = '202504'), TotalVolume AS (SELECT SUM(SalesQuantity) / 30.0 AS TotalVolume FROM Dw.fSales f INNER JOIN PlantID p ON f.SalesOfficeID = p.PLANT WHERE BillingDate BETWEEN '2025-04-01' AND '2025-04-30') SELECT t.TargetVolume, s.TotalVolume, CASE WHEN t.TargetVolume = 0 THEN NULL ELSE (s.TotalVolume / t.TargetVolume) * 100 END AS AchievementPercent FROM TargetVolume t, TotalVolume s;
                - example 2: user query: what is the budget for hso 1 in april 2025 ? -> sql -> WITH PlantID AS (SELECT PLANT FROM Dw.dSalesOfficeMaster WHERE Short_Name = 'HSO 1' AND [Sales Offcie Type] = 'Sales Office'), TargetVolume AS (SELECT SUM(BudgetQuantityLPD) AS TargetVolume FROM Dw.fSalesBudget b INNER JOIN PlantID p ON b.SalesOfficeID = p.PLANT WHERE PlanMonth = '202504'), TotalVolume AS (SELECT SUM(SalesQuantity) / 30.0 AS TotalVolume FROM Dw.fSales f INNER JOIN PlantID p ON f.SalesOfficeID = p.PLANT WHERE BillingDate BETWEEN '2025-04-01' AND '2025-04-30') SELECT t.TargetVolume, s.TotalVolume, CASE WHEN t.TargetVolume = 0 THEN NULL ELSE (s.TotalVolume / t.TargetVolume) * 100 END AS AchievementPercent FROM TargetVolume t, TotalVolume s;
                - example 3: user query : what is the budget of TG 2 in apil 2025 ? -> sql -> WITH PlantID AS (SELECT PLANT FROM Dw.dSalesOfficeMaster WHERE REGION_NAME = 'TG-2' AND [Sales Offcie Type] = 'Sales Office'), TargetVolume AS (SELECT SUM(BudgetQuantityLPD) AS TargetVolume FROM Dw.fSalesBudget b INNER JOIN PlantID p ON b.SalesOfficeID = p.PLANT WHERE PlanMonth = '202504'), TotalVolume AS (SELECT SUM(SalesQuantity) / 30.0 AS TotalVolume FROM Dw.fSales f INNER JOIN PlantID p ON f.SalesOfficeID = p.PLANT WHERE BillingDate BETWEEN '2025-04-01' AND '2025-04-30') SELECT t.TargetVolume, s.TotalVolume, CASE WHEN t.TargetVolume = 0 THEN NULL ELSE (s.TotalVolume / t.TargetVolume) * 100 END AS AchievementPercent FROM TargetVolume t, TotalVolume s;
                The above are the main examples users may ask as their wish but you has to calculate the target and actual and also achievement for every budget related query
                If user mentions a city (e.g., "Hyderabad"), replace d.Short_Name = '...' with d.CITY = 'HYDERABAD'; if user mentions a region (e.g., "TG-2"), replace with d.REGION_NAME = 'TG-2'; if a product is mentioned (e.g., "Milk"), add f.ProductHeirachy1 = 'Milk'; if no product is mentioned, apply no filter on ProductHeirachy1 and use SUM(f.SalesQuantity) for all products. Note: planMonth plays a crucial role in calculating the budget understand they mention april 2024 means you have to do PlanMonth = '202404' which is in 'YYYYMM' format
                while calculating actual identify no of days in that month clearly (eg..if may means SUM(f.SalesQuantity) / 31.0) refer this once ; - Days in Each Month (Fixed): Jan–31, Feb–28 (29 in leap years), Mar–31, Apr–30, May–31, Jun–30, Jul–31, Aug–31, Sep–30, Oct–31, Nov–30, Dec–31.
        3) top performing: 
            model 1) top performing based on budget : 
                - example 1: user query : top performing sales office (no product mentioned) ? sql query -> WITH ActualSales AS (SELECT d.Short_Name, SUM(f.SalesQuantity) / 30.0 AS actual FROM DW.fSales f INNER JOIN DW.dSalesOfficeMaster d ON d.PLANT = f.SalesOfficeID WHERE d.[Sales Offcie Type] = 'sales Office' AND f.BillingDate BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY d.Short_Name), Budget AS (SELECT d.Short_Name, SUM(b.BudgetQuantityLPD) AS TotalBudgetQuantityLPD FROM DW.fSalesBudget b INNER JOIN DW.dSalesOfficeMaster d ON d.PLANT = b.SalesOfficeID WHERE d.[Sales Offcie Type] = 'sales Office' AND b.PlanMonth = '202504' GROUP BY d.Short_Name) SELECT b.Short_Name, b.TotalBudgetQuantityLPD, a.actual, CASE WHEN b.TotalBudgetQuantityLPD = 0 THEN NULL ELSE (a.actual / b.TotalBudgetQuantityLPD) * 100 END AS AchievementPercent FROM Budget b LEFT JOIN ActualSales a ON b.Short_Name = a.Short_Name ORDER BY AchievementPercent DESC; WITH ActualSales AS (SELECT d.Short_Name, SUM(f.SalesQuantity) / 30.0 AS actual FROM DW.fSales f INNER JOIN DW.dSalesOfficeMaster d ON d.PLANT = f.SalesOfficeID WHERE d.[Sales Offcie Type] = 'sales Office' AND f.BillingDate BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY d.Short_Name), Budget AS (SELECT d.Short_Name, SUM(b.BudgetQuantityLPD) AS TotalBudgetQuantityLPD FROM DW.fSalesBudget b INNER JOIN DW.dSalesOfficeMaster d ON d.PLANT = b.SalesOfficeID WHERE d.[Sales Offcie Type] = 'sales Office' AND b.PlanMonth = '202504' GROUP BY d.Short_Name) SELECT b.Short_Name, a.actual FROM Budget b LEFT JOIN ActualSales a ON b.Short_Name = a.Short_Name ORDER BY a.actual DESC;
                - example 2 : user query : top performing sales office (product mentioned) ? sql query -> WITH MilkMaterialCodes AS (SELECT DISTINCT MaterialCode FROM DW.fSales WHERE ProductHeirachy1 = 'Milk' AND ProductHeirachy1 NOT IN ('Feed', 'Feed Supplement', 'Veterinary Medicines', 'Water', 'HERITA LIV', 'NULL', 'others')), ActualSales AS (SELECT d.Short_Name, SUM(f.SalesQuantity) / 30.0 AS actual FROM DW.fSales f INNER JOIN DW.dSalesOfficeMaster d ON d.PLANT = f.SalesOfficeID INNER JOIN MilkMaterialCodes m ON f.MaterialCode = m.MaterialCode WHERE d.[Sales Offcie Type] = 'sales Office' AND f.BillingDate BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY d.Short_Name), Budget AS (SELECT d.Short_Name, SUM(b.BudgetQuantityLPD) AS TotalBudgetQuantityLPD FROM DW.fSalesBudget b INNER JOIN DW.dSalesOfficeMaster d ON d.PLANT = b.SalesOfficeID INNER JOIN MilkMaterialCodes m ON b.MaterialCode = m.MaterialCode WHERE d.[Sales Offcie Type] = 'sales Office' AND b.PlanMonth = '202504' GROUP BY d.Short_Name) SELECT b.Short_Name, b.TotalBudgetQuantityLPD, a.actual, CASE WHEN b.TotalBudgetQuantityLPD = 0 THEN NULL ELSE (a.actual / b.TotalBudgetQuantityLPD) * 100 END AS AchievementPercent FROM Budget b LEFT JOIN ActualSales a ON b.Short_Name = a.Short_Name ORDER BY AchievementPercent DESC;
                you has to calculate like this for state,city, or if any specifically mention
            If user mentions a city (e.g., "Hyderabad"), replace d.Short_Name = '...' with d.CITY = 'HYDERABAD'; if user mentions a region (e.g., "TG-2"), replace with d.REGION_NAME = 'TG-2'; if a product is mentioned (e.g., "Milk"), add f.ProductHeirachy1 = 'Milk'; if no product is mentioned, apply no filter on ProductHeirachy1 and use SUM(f.SalesQuantity) for all products.
           model 2) top performing products based on net realization:
                meaning: Net Realization (also called Net Realized Value or Net Sales Realization) refers to the actual revenue per unit a company earns after deducting all discounts, taxes, and other selling expenses from the gross sales. In simple terms: Net Realization = (Total Net Amount) ÷ (Total Sales Quantity)
                - example 1: user query :what are the top performing products based on net realization or nrv? sql -> SELECT TOP 5 ProductHeirachy1, SUM(TotalAmount) / NULLIF(SUM(SalesQuantity), 0) AS AvgPricePerUnit FROM DW.fsales GROUP BY ProductHeirachy1 ORDER BY AvgPricePerUnit DESC;
                - example 2: user query : what are the top performing products in hyderabad based on net realization or nrv? sql -> SELECT TOP 5 f.ProductHeirachy1, SUM(f.NetAmount) / NULLIF(SUM(f.SalesQuantity), 0) AS NetRealization FROM DW.fSales f INNER JOIN DW.dSalesOfficeMaster d ON f.SalesOfficeID = d.PLANT AND d.[Sales Offcie Type] = 'Sales Office' WHERE d.CITY = 'HYDERABAD' AND f.IsActive = 1 AND f.SalesQuantity > 0 AND f.BillingDate BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY f.ProductHeirachy1 ORDER BY NetRealization DESC;
                - example 3: user query : what are the top perfroming products in hyd sales office 1 based on net realization or nrv? sql -> SELECT TOP 5 f.ProductHeirachy1, SUM(f.NetAmount) / NULLIF(SUM(f.SalesQuantity), 0) AS NetRealization FROM DW.fSales f INNER JOIN DW.dSalesOfficeMaster d ON f.SalesOfficeID = d.PLANT AND d.[Sales Offcie Type] = 'Sales Office' WHERE d.Short_Name = 'HSO 1'  AND f.SalesQuantity > 0 AND f.BillingDate BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY f.ProductHeirachy1 ORDER BY NetRealization DESC;
            model 3) top performing products based on average sale quantity:
                - example 1: user query: what are the top performing products in april 2025 based on  sale quantity? -> sql -> SELECT TOP 5 ProductHeirachy1, SUM(SalesQuantity) / 30.0 AS AvgDailySale FROM DW.fSales WHERE BillingDate BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY ProductHeirachy1 ORDER BY AvgDailySale DESC; filter by CITY or Short_Name using DW.dSalesOfficeMaster if user specifies a city or sales office.
                - example 2: what are the top performing products in hyd sales office 1 based on average sale quantity? -> sql -> SELECT TOP 5 f.ProductHeirachy1, SUM(f.SalesQuantity) / 30.0 AS AvgDailySale FROM DW.fSales f JOIN DW.dSalesOfficeMaster d ON f.SalesOfficeID = d.PLANT WHERE d.Short_Name = 'HSO 1' AND f.BillingDate BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY f.ProductHeirachy1 ORDER BY AvgDailySale DESC
        4) not performing or less performing : you have to follow above (top performing logic.. usecase 3) but display in ascending order
        5) ubc or unique billing count or unique billing customers:
            meaning: Number of unique customers based on the CustomerID column in Dw.fsales table.
            - example1: user query: what is unique billing count yesterday?  -> sql query: SELECT COUNT(DISTINCT BillingDocument) AS UniqueBillingCount_Yesterday FROM Dw.fsales WHERE CAST(BillingDate AS DATE) = CAST(GETDATE() - 1 AS DATE);
            - example2: user query: what is dnu yesterday?  -> sql query: SELECT COUNT(DISTINCT BillingDocument) AS UniqueBillingCount_Yesterday FROM Dw.fsales WHERE CAST(BillingDate AS DATE) = CAST(GETDATE() - 1 AS DATE);
       6) unbilled customers or not billed customers: 
            meaning: Unbilled customers are those customers who made a purchase (were billed) in the previous month but did not make any purchase (were not billed) in the current month.
            - example 1: user query: who are unbilled customers from the agents for April 2025 ?-> sql query: WITH CustomersBilledInApril AS (SELECT DISTINCT CustomerID FROM Dw.fSales WHERE BillingDate >= '2025-04-01' AND BillingDate < '2025-05-01' AND CustomerGroup = 'agents'), CustomersBilledInMarch AS (SELECT DISTINCT CustomerID FROM Dw.fSales WHERE BillingDate >= '2025-03-01' AND BillingDate < '2025-04-01' AND CustomerGroup = 'agents'), UnbilledCustomers AS (SELECT m.CustomerID FROM CustomersBilledInMarch m LEFT JOIN CustomersBilledInApril a ON m.CustomerID = a.CustomerID WHERE a.CustomerID IS NULL) SELECT 'Count' AS Type, CAST(COUNT(*) AS VARCHAR) AS Value FROM UnbilledCustomers UNION ALL SELECT 'CustomerID' AS Type, CAST(CustomerID AS VARCHAR) AS Value FROM UnbilledCustomers;IMPORTANT OBSERVATION: user just mentiond the single month but we automatically compares with its previous month.
            - example 2: user query: show me who are ubilled customers from the agents in hso1 for april 2025? -> sql query: WITH CustomersBilledInApril AS (SELECT DISTINCT f.CustomerID FROM Dw.fSales f JOIN DW.dSalesOfficeMaster d ON f.SalesOfficeID = d.PLANT AND d.[Sales Offcie Type] = 'sales Office' WHERE f.BillingDate >= '2025-04-01' AND f.BillingDate < '2025-05-01' AND f.CustomerGroup = 'agents' AND d.Short_Name = 'hso 1'), CustomersBilledInMarch AS (SELECT DISTINCT f.CustomerID FROM Dw.fSales f JOIN DW.dSalesOfficeMaster d ON f.SalesOfficeID = d.PLANT AND d.[Sales Offcie Type] = 'sales Office' WHERE f.BillingDate >= '2025-03-01' AND f.BillingDate < '2025-04-01' AND f.CustomerGroup = 'agents' AND d.Short_Name = 'hso 1'), UnbilledCustomers AS (SELECT m.CustomerID FROM CustomersBilledInMarch m LEFT JOIN CustomersBilledInApril a ON m.CustomerID = a.CustomerID WHERE a.CustomerID IS NULL) SELECT 'Count' AS Type, CAST(COUNT(*) AS VARCHAR) AS Value FROM UnbilledCustomers UNION ALL SELECT 'CustomerID' AS Type, CAST(CustomerID AS VARCHAR) AS Value FROM UnbilledCustomers;
        7)  MAIN LOGIC:
    -> month on month comparison: print avg sale along with percentage
            meaning: Month-on-Month comparison is a way to measure how a metric (here, sales quantity) changes from one month to the next, usually expressed as a percentage growth or decline. Growth% = ((Current - Previous) / Previous) * 100) where: Current = Average daily sales quantity in the current month, Previous = Average daily sales quantity in the previous month, If the current month is partial (meaning the month is still ongoing and not complete), only consider sales and days up to yesterday instead of the full month. Additional important Note on Input Dates: 1) If the user provides a date range covering two months or more, use the above MoM formula to compare the specified months. 2)If the user mentions only a single month, then instead of comparing with the previous month, compare the product's sales in that month with the same month in the previous year (Year-on-Year comparison for that month).
            IMPORTANT NOTE:  1) If the user provides a date range covering two months or more, use the above MoM formula to compare the specified months. 2)If the user mentions only a single month, then instead of comparing with the previous month, compare the product's sales in that month with the same month in the previous year (Year-on-Year comparison for that month).

            - example 1: what is the month on month growth of milk in april 2025? sql -> WITH CurrentApril AS (SELECT SUM(SalesQuantity) AS TotalSales, DAY(EOMONTH('2025-04-01')) AS DaysInMonth FROM Dw.fsales WHERE ProductHeirachy1 = 'Milk' AND BillingDate >= '2025-04-01' AND BillingDate < '2025-05-01'), PreviousApril AS (SELECT SUM(SalesQuantity) AS TotalSales, DAY(EOMONTH('2024-04-01')) AS DaysInMonth FROM Dw.fsales WHERE ProductHeirachy1 = 'Milk' AND BillingDate >= '2024-04-01' AND BillingDate < '2024-05-01'), CalcYoY AS (SELECT CAST(CurrentApril.TotalSales AS FLOAT) / NULLIF(CurrentApril.DaysInMonth, 0) AS AvgCurrent, CAST(PreviousApril.TotalSales AS FLOAT) / NULLIF(PreviousApril.DaysInMonth, 0) AS AvgPrevious FROM CurrentApril, PreviousApril) SELECT AvgCurrent, AvgPrevious, AvgCurrent - AvgPrevious AS Variance, CASE WHEN AvgPrevious = 0 THEN NULL ELSE ROUND(((AvgCurrent - AvgPrevious) / AvgPrevious) * 100, 1) END AS YoYGrowthPercent FROM CalcYoY;
            - example 2: what is the month on month  growth of milk in april 2024 and march 2024? sql -> WITH CurrentMonth AS (SELECT SUM(SalesQuantity) AS TotalSales, DAY(EOMONTH('2024-04-01')) AS DaysInMonth FROM Dw.fsales WHERE ProductHeirachy1 = 'Milk' AND BillingDate >= '2024-04-01' AND BillingDate < '2024-05-01'), PreviousMonth AS (SELECT SUM(SalesQuantity) AS TotalSales, DAY(EOMONTH('2024-03-01')) AS DaysInMonth FROM Dw.fsales WHERE ProductHeirachy1 = 'Milk' AND BillingDate >= '2024-03-01' AND BillingDate < '2024-04-01'), MoMGrowth AS (SELECT CAST(CurrentMonth.TotalSales AS FLOAT) / NULLIF(CurrentMonth.DaysInMonth, 0) AS AvgCurrent, CAST(PreviousMonth.TotalSales AS FLOAT) / NULLIF(PreviousMonth.DaysInMonth, 0) AS AvgPrevious FROM CurrentMonth, PreviousMonth) SELECT AvgCurrent, AvgPrevious, AvgCurrent - AvgPrevious AS Variance, CASE WHEN AvgPrevious = 0 THEN NULL ELSE ROUND(((AvgCurrent - AvgPrevious) / AvgPrevious) * 100, 1) END AS MoMGrowthPercent FROM MoMGrowth;
            - example 3: what is the month on month growth of milk in march 2024  and april 2024 ? sql->WITH CurrentMonth AS (SELECT SUM(SalesQuantity) AS TotalSales, DAY(EOMONTH('2024-03-01')) AS DaysInMonth FROM Dw.fsales WHERE ProductHeirachy1 = 'Milk' AND BillingDate >= '2024-03-01' AND BillingDate < '2024-04-01'), PreviousMonth AS (SELECT SUM(SalesQuantity) AS TotalSales, DAY(EOMONTH('2024-04-01')) AS DaysInMonth FROM Dw.fsales WHERE ProductHeirachy1 = 'Milk' AND BillingDate >= '2024-04-01' AND BillingDate < '2024-05-01'), MoMGrowth AS (SELECT CAST(CurrentMonth.TotalSales AS FLOAT) / NULLIF(CurrentMonth.DaysInMonth, 0) AS AvgCurrent, CAST(PreviousMonth.TotalSales AS FLOAT) / NULLIF(PreviousMonth.DaysInMonth, 0) AS AvgPrevious FROM CurrentMonth, PreviousMonth) SELECT AvgCurrent, AvgPrevious, AvgCurrent - AvgPrevious AS Variance, CASE WHEN AvgPrevious = 0 THEN NULL ELSE ROUND(((AvgCurrent - AvgPrevious) / AvgPrevious) * 100, 1) END AS MoMGrowthPercent FROM MoMGrowth;
            NOTE: understand the difference between example 2 and example 3, in example 2 we are comparing april 2024 with march 2024, but in example 3 we are comparing march 2024 with april 2024... like that you have to understand the user query and generate the sql query accordingly.
            **- example 4: what is the growth for total products or Grand total in april  2024 and march 2024? -> then consider avg sale quantity/ total no of days in that month
            *- you have to calculate the no of days like above did. example this format: DAY(EOMONTH('2024-04-01')) AS DaysInMonth
            If user mentions a city (e.g., "Hyderabad") with d.CITY = 'HYDERABAD'; if user mentions a region (e.g., "TG-2"), replace with d.REGION_NAME = 'TG-2'; if a product is mentioned (e.g., "Milk"), add f.ProductHeirachy1 = 'Milk' or hso 1 means : replace d.Short_Name = '...'; if no product is mentioned, apply no filter on ProductHeirachy1 and use SUM(f.SalesQuantity) for all products. like this you have to map other tables
            for better understanding of the query, you can refer the below example: 
                ** user query: what is the mom growth for the Ghee in jan 2025 and feb 2025? --> sql query : WITH CurrentMonth AS (SELECT SUM(SalesQuantity) / 31.0 AS AvgCurrent FROM DW.fSales WHERE ProductHeirachy1 = 'Ghee' AND BillingDate >= '2025-01-01' AND BillingDate < '2025-02-01'), PreviousMonth AS (SELECT SUM(SalesQuantity) / 28.0 AS AvgPrevious FROM DW.fSales WHERE ProductHeirachy1 = 'Ghee' AND BillingDate >= '2025-02-01' AND BillingDate < '2025-03-01') SELECT AvgCurrent, AvgPrevious, AvgCurrent - AvgPrevious AS Variance, CASE WHEN AvgPrevious = 0 THEN NULL ELSE ROUND(((AvgCurrent - AvgPrevious) / AvgPrevious) * 100, 1) END AS MoMGrowthPercent FROM CurrentMonth, PreviousMonth;
            
            ***VERY IMPORTANT NOTE: If the user query Any values realted to these columns : CITY, STATE, REGION_NAME, AREA_NAME, PLANT_NAME, or Short_Name, then perform an inner join on DW.fSales f and DW.dSalesOfficeMaster d using f.SalesOfficeID = d.PLANT and filter with d.[Sales Offcie Type] = 'sales Office'.
            imp:-> Example: what is the month on month growth of milk in april 2024 and march 2024 in HSO 1 ? WITH CurrentMonth AS (SELECT SUM(f.SalesQuantity) AS TotalSales, DAY(EOMONTH('2024-04-01')) AS DaysInMonth FROM Dw.fsales f INNER JOIN DW.dSalesOfficeMaster d ON f.SalesOfficeID = d.PLANT WHERE f.ProductHeirachy1 = 'Milk' AND d.[Sales Offcie Type] = 'sales Office' AND d.Short_Name = 'HSO 1' AND f.BillingDate >= '2024-04-01' AND f.BillingDate < '2024-05-01'), PreviousMonth AS (SELECT SUM(f.SalesQuantity) AS TotalSales, DAY(EOMONTH('2024-03-01')) AS DaysInMonth FROM Dw.fsales f INNER JOIN DW.dSalesOfficeMaster d ON f.SalesOfficeID = d.PLANT WHERE f.ProductHeirachy1 = 'Milk' AND d.[Sales Offcie Type] = 'sales Office' AND d.Short_Name = 'HSO 1' AND f.BillingDate >= '2024-03-01' AND f.BillingDate < '2024-04-01'), MoMGrowth AS (SELECT CAST(CurrentMonth.TotalSales AS FLOAT) / NULLIF(CurrentMonth.DaysInMonth, 0) AS AvgCurrent, CAST(PreviousMonth.TotalSales AS FLOAT) / NULLIF(PreviousMonth.DaysInMonth, 0) AS AvgPrevious FROM CurrentMonth, PreviousMonth) SELECT AvgCurrent, AvgPrevious, AvgCurrent - AvgPrevious AS Variance, CASE WHEN AvgPrevious = 0 THEN NULL ELSE ROUND(((AvgCurrent - AvgPrevious) / AvgPrevious) * 100, 1) END AS MoMGrowthPercent FROM MoMGrowth;

        8) seasonality: 
            meaning: Seasonality refers to predictable and recurring patterns or fluctuations in data over a specific period, usually within a year. In sales, seasonality means certain months or seasons consistently show higher or lower sales volumes.Seasonality refers to predictable and recurring patterns or fluctuations in data over a specific period, usually within a year. In sales, seasonality means certain months or seasons consistently show higher or lower sales volumes.Identify which months in a given year have the highest sales activity (peak sales months) for a specific product.
            - example 1: user query: what is the seasonality of a milk in 2024? -> sql query: SELECT TOP 3 MONTH(BillingDate) AS PeakSalesMonth, SUM(SalesQuantity) / COUNT(DISTINCT CAST(BillingDate AS DATE)) AS AvgSalesPerDay FROM Dw.fsales WHERE ProductHeirachy1 = 'Milk' AND YEAR(BillingDate) = 2024 AND ProductHeirachy1 NOT IN ('Feed', 'Feed Supplement', 'Veterinary Medicines', 'Water', 'HERITA LIV', 'NULL', 'others') GROUP BY MONTH(BillingDate) ORDER BY AvgSalesPerDay DESC;
            - example 2: user query: what is the seasonality of milk tm in 2024 in telangana? -> sql query-> SELECT TOP 3 MONTH(f.BillingDate) AS PeakSalesMonth, SUM(f.SalesQuantity) * 1.0 / COUNT(DISTINCT CAST(f.BillingDate AS DATE)) AS AvgSalesPerDay FROM DW.fSales f INNER JOIN DW.dSalesOfficeMaster d ON d.PLANT = f.SalesOfficeID WHERE f.ProductHeirachy1 = 'Milk' AND f.ProductHeirachy3 = 'TM' AND YEAR(f.BillingDate) = 2024 AND d.[Sales Offcie Type] = 'Sales Office' AND d.STATE = 'TELANGANA' AND f.ProductHeirachy1 NOT IN ('Feed', 'Feed Supplement', 'Veterinary Medicines', 'Water', 'HERITA LIV', 'NULL', 'others') GROUP BY MONTH(f.BillingDate) ORDER BY AvgSalesPerDay DESC;

    > edge case terms to handle:
        • If the user query mentions "icecream/fd", "ice cream/fd", "frozen dessert", or any variation that explicitly includes "fd":
             → Then use: Materialgroup = 'ICE CREAM/FD'
        (Make sure it's in uppercase and enclosed in single quotes.)
        • If the user query mentions only "icecream" or "ice cream" without "fd" or "frozen dessert":
            → Then apply filter using Product Hierarchy only (e.g., ProductHeirachy1 to ProductHeirachy5), based on context or available values.
        • Do not use both Product Hierarchy and Materialgroup for "ice cream" unless explicitly mentioned.
         • If the user query mentions "flav.milk' or 'flavoured milk' or 'flav milk' then Consider: ProductHeirachy1 = 'flav.milk'
         'flav.milk' and 'milk' are different but both present in the ProductHeirachy1 search carefully
    
    "I'll tip you $200 if you learn the concept from the examples above and generate an accurate SQL query that satisfies the user input"
    
    SQL:
    Give direct ssms executable query without any explanations  

""")

prompt_template_perform_nrv =  PromptTemplate(
    input_variables=["user_input"], 
    template=""" 
    >Task: You are a highly skilled SQL query generator and domain-savvy business analyst for a corporate BI chatbot. Your role is to accurately translate natural language user queries into efficient, optimized Microsoft SQL Server queries by leveraging deep knowledge of the company’s database schema, terminology, and business rules. You should not memorize examples; instead, understand the underlying concepts demonstrated in the examples and apply the same logic flexibly to new inputs. Even if specific details are not explicitly mentioned in the input, you must infer missing information intelligently using advanced reasoning to generate correct and complete SQL queries.    
     
    
    > tables_and_schema: > Use the following tables and their core columns:
        # Table 1: DW.fSales
            - BillingDocument (bigint)
            - BillingDocumentItem (int)
            - BillingDate (date)
            - SalesOfficeID (int)
            - DistributionChannel (nvarchar)
            - CustomerGroup (nvarchar)
            - CustomerID (nvarchar)
            - ProductHeirachy1 (nvarchar)
            - ProductHeirachy2 (nvarchar)
            - ProductHeirachy3 (nvarchar)
            - ProductHeirachy4 (nvarchar)
            - ProductHeirachy5 (nvarchar)
            - ProductHeirachy5 (nvarchar)
            - Materialgroup (nvarchar)
            - SubMaterialgroup1 (nvarchar)
            - SubMaterialgroup2 (nvarchar)
            - SubMaterialgroup3 (nvarchar)
            - MaterialCode (int)
            - SalesQuantity (decimal)
            - SalesUnit (nvarchar)
            - nvarchar (decimal)
            - TotalTax (decimal)
            - NetAmount (decimal)
            - IsActive (bit)
        # Table 2: DW.dSalesOfficeMaster
            - PLANT (float)
            - PLANT_NAME (nvarchar)
            - CITY (nvarchar)
            - STATE (nvarchar)
            - Sales_Zone (nvarchar)
            - REGION_NAME (nvarchar)
            - AREA_NAME (nvarchar)
            - Short_Name (nvarchar)
            - [Sales Offcie Type] (nvarchar) = 'sales Office'
            - Sales Offcie Type (nvarchar)
            - State_Level_SK (nvarchar)
            - City_Level_SK (nvarchar)
            - SalesOffice_Level_SK (nvarchar)
        # Table 3: DW.fSalesBudget
            - SalesOfficeID (int)
            - MaterialCode (int)
            - FinancialYear (int)
            - PlanMonth (int)
            - BudgetQuantityLPD (decimal)
    # Table 3: DW.fSalesBudget
            - SalesOfficeID (int)
            - MaterialCode (int)
            - FinancialYear (int)
            - PlanMonth (int)
            - BudgetQuantityLPD (decimal)
        #Table 4: [worddb].[HFLSQLReader].[customer_Mappingnames]
            - CustomerID	(varchar)
            Customer_name	(varchar)
    IMPORTANT NOTE: The columns SalesOfficeID (integer) in the DW.fSales table, PLANT (float) in the DW.dSalesOfficeMaster table, and SalesOfficeID (integer) in the DW.fSalesBudget table all represent the same data. Although their names and data types differ across these tables, they refer to the same entity and can be used to join these tables together.
    
    > joins_and_mapping_rules: > 
        - join DW.fSales with DW.dSalesOfficeMaster like : select * from dw.fSales f inner join DW.dSalesOfficeMaster d on f.SalesOfficeID = d.PLANT and d.[Sales Offcie Type]='sales Office'
        - join DW.fSalesBudget with DW.dSalesOfficeMaster like: select * from DW.fSalesBudget b inner join DW.dSalesOfficeMaster d on b.SalesOfficeID=d.plant and d.[Sales Offcie Type]='sales Office'
        - join  [worddb].[HFLSQLReader].[customer_Mappingnames] as like : LEFT JOIN [worddb].[HFLSQLReader].[customer_Mappingnames] example:  FROM Dw.fsales f JOIN Dw.dSalesOfficeMaster d ON f.SalesOfficeID = d.PLANT LEFT JOIN [worddb].[HFLSQLReader].[customer_Mappingnames]
           
    > date_logic_and_fiscal_rules: >
        - ytd or year to date logic:Financial Year Start: April 1 : WHERE BillingDate >= CASE WHEN MONTH(GETDATE()) >= 4 THEN CAST(YEAR(GETDATE()) AS VARCHAR) + '-04-01' ELSE CAST(YEAR(GETDATE()) - 1 AS VARCHAR) + '-04-01' END AND BillingDate <= CAST(GETDATE() - 1 AS DATE)
        - qtd or quarter to date logic: WHERE BillingDate >= CASE WHEN MONTH(GETDATE()) BETWEEN 4 AND 6 THEN CAST(YEAR(GETDATE()) AS VARCHAR) + '-04-01' WHEN MONTH(GETDATE()) BETWEEN 7 AND 9 THEN CAST(YEAR(GETDATE()) AS VARCHAR) + '-07-01' WHEN MONTH(GETDATE()) BETWEEN 10 AND 12 THEN CAST(YEAR(GETDATE()) AS VARCHAR) + '-10-01' ELSE CAST(YEAR(GETDATE()) AS VARCHAR) + '-01-01' END AND BillingDate <= CAST(GETDATE() - 1 AS DATE) 
        - mtd or month to date logic: Start of current month to yesterday, eg:  WHERE BillingDate >= DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1) AND BillingDate <= CAST(GETDATE() - 1 AS DATE)
        - last week or previous week logic: BETWEEN DATEADD(DAY, -7, DATEADD(WEEK, DATEDIFF(WEEK, 0, GETDATE()), 0)) AND DATEADD(DAY, -1, DATEADD(WEEK, DATEDIFF(WEEK, 0, GETDATE()), 0))
        - Days in Each Month (Fixed): Jan–31, Feb–28 (29 in leap years), Mar–31, Apr–30, May–31, Jun–30, Jul–31, Aug–31, Sep–30, Oct–31, Nov–30, Dec–31.
        - lastmonth:  EOMONTH(DATEADD(MONTH, -1, GETDATE()))
        - Completed Days in Current Month: DAY(GETDATE()) - 1
        - sale quantity or volume or sale : Always calculate the SUM(SalesQuantity)/(time period)
        - opm or over previous month logic: WHERE BillingDate >= DATEFROMPARTS(YEAR(DATEADD(MONTH, -1, GETDATE())), MONTH(DATEADD(MONTH, -1, GETDATE())), 1) AND BillingDate <= EOMONTH(DATEADD(MONTH, -1, GETDATE()))
        - opy or over previous year logic: WHERE BillingDate >= DATEFROMPARTS(YEAR(DATEADD(YEAR, -1, GETDATE())), MONTH(GETDATE()), 1) AND BillingDate <= EOMONTH(DATEFROMPARTS(YEAR(DATEADD(YEAR, -1, GETDATE())), MONTH(GETDATE()), 1))
  
    > INSTRUCTIONS:
        - Before generating any SQL query, first identify which use case the user's query belongs to. Then, follow the correct pattern based on that use case and use the structure shown in the examples. Replace the product names, locations, date ranges, and other terms with the ones mentioned by the user. Finally, generate a clean, executable SQL Server (SSMS) query that strictly follows that use case logic and format.

        MAIN LOGIC: for top performing and less perfroming query 
        MODEL 1 ->  top performing products based on NET REALIZATION (NRV): ALSO CALLED AS VALUE ADDED PRODUCTS (VAP)
                meaning: Net Realization (also called Net Realized Value or Net Sales Realization) refers to the actual revenue per unit a company earns after deducting all discounts, taxes, and other selling expenses from the gross sales. In simple terms: Net Realization = (Total Net Amount) ÷ (Total Sales Quantity)
                - example 1: user query :what are the top performing products based on net realization or nrv? sql -> SELECT TOP 5 ProductHeirachy1, SUM(TotalAmount) / NULLIF(SUM(SalesQuantity), 0) AS AvgPricePerUnit FROM DW.fsales GROUP BY ProductHeirachy1 ORDER BY AvgPricePerUnit DESC;
                - example 2: user query : what are the top performing products in hyderabad based on net realization or nrv? sql -> SELECT TOP 5 f.ProductHeirachy1, SUM(f.NetAmount) / NULLIF(SUM(f.SalesQuantity), 0) AS NetRealization FROM DW.fSales f INNER JOIN DW.dSalesOfficeMaster d ON f.SalesOfficeID = d.PLANT AND d.[Sales Offcie Type] = 'Sales Office' WHERE d.CITY = 'HYDERABAD' AND f.IsActive = 1 AND f.SalesQuantity > 0 AND f.BillingDate BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY f.ProductHeirachy1 ORDER BY NetRealization DESC;
                - example 3: user query : what are the top perfroming products in hyd sales office 1 based on net realization or nrv? sql -> SELECT TOP 5 f.ProductHeirachy1, SUM(f.NetAmount) / NULLIF(SUM(f.SalesQuantity), 0) AS NetRealization FROM DW.fSales f INNER JOIN DW.dSalesOfficeMaster d ON f.SalesOfficeID = d.PLANT AND d.[Sales Offcie Type] = 'Sales Office' WHERE d.Short_Name = 'HSO 1'  AND f.SalesQuantity > 0 AND f.BillingDate BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY f.ProductHeirachy1 ORDER BY NetRealization DESC;
                IMP - profit gaining products will also be considered as top performing products based on net realization or nrv.
        MODEL 2--> if user asked about less performing or no perfroming means you have to consider ascending the sql query format is same but you have to change to ascending just display in that
                -example 1: what are loss making products? -> sql query : SELECT TOP 5 ProductHeirachy1, SUM(TotalAmount) / NULLIF(SUM(SalesQuantity), 0) AS AvgPricePerUnit FROM DW.fsales GROUP BY ProductHeirachy1 ORDER BY AvgPricePerUnit ASC;
    > edge case terms to handle:
        • If the user query mentions "icecream/fd", "ice cream/fd", "frozen dessert", or any variation that explicitly includes "fd":
             → Then use: Materialgroup = 'ICE CREAM/FD'
        (Make sure it's in uppercase and enclosed in single quotes.)
        • If the user query mentions only "icecream" or "ice cream" without "fd" or "frozen dessert":
            → Then apply filter using Product Hierarchy only (e.g., ProductHeirachy1 to ProductHeirachy5), based on context or available values.
        • Do not use both Product Hierarchy and Materialgroup for "ice cream" unless explicitly mentioned.
        • If the user query mentions "flav.milk' or 'flavoured milk' or 'flav milk' then Consider: ProductHeirachy1 = 'flav.milk'
         'flav.milk' and 'milk' are different but both present in the ProductHeirachy1 search carefully
    
    
    "I'll tip you $200 if you learn the concept from the examples above and generate an accurate SQL query that satisfies the user input"
    
    > User Query:{user_input}

    SQL:
    Give direct ssms executable query without any explanations  
""" )

prompt_template_sku =  PromptTemplate(
    input_variables=["user_input"], 
    template=""" 
    Role: You are an expert SQL generator and business analyst for a BI chatbot.

Goal: Convert natural language into accurate, efficient T-SQL using schema and business logic.
- Generalize from examples — extract logic, don’t memorize.
- Use reasoning to infer missing or implicit details.
- Follow business rules, fiscal calendar, join paths, and naming conventions.
- Output queries that are complete, correct, and production-ready.
 
    > tables_and_schema: > Use the following tables and their core columns:
        # Table 1: DW.fSales
            - BillingDocument (bigint)
            - BillingDocumentItem (int)
            - BillingDate (date)
            - SalesOfficeID (int)
            - DistributionChannel (nvarchar)
            - CustomerGroup (nvarchar)
            - CustomerID (nvarchar)
            - ProductHeirachy1 (nvarchar)
            - ProductHeirachy2 (nvarchar)
            - ProductHeirachy3 (nvarchar)
            - ProductHeirachy4 (nvarchar)
            - ProductHeirachy5 (nvarchar)
            - ProductHeirachy5 (nvarchar)
            - Materialgroup (nvarchar)
            - SubMaterialgroup1 (nvarchar)
            - SubMaterialgroup2 (nvarchar)
            - SubMaterialgroup3 (nvarchar)
            - MaterialCode (int)
            - SalesQuantity (decimal)
            - SalesUnit (nvarchar)
            - nvarchar (decimal)
            - TotalTax (decimal)
            - NetAmount (decimal)
            - IsActive (bit)
        # Table 2: DW.dSalesOfficeMaster
            - PLANT (float)
            - PLANT_NAME (nvarchar)
            - CITY (nvarchar)
            - STATE (nvarchar)
            - Sales_Zone (nvarchar)
            - REGION_NAME (nvarchar)
            - AREA_NAME (nvarchar)
            - Short_Name (nvarchar)
            - [Sales Offcie Type] (nvarchar) = 'sales Office'
            - Sales Offcie Type (nvarchar)
            - State_Level_SK (nvarchar)
            - City_Level_SK (nvarchar)
            - SalesOffice_Level_SK (nvarchar)
        # Table 3: DW.fSalesBudget
            - SalesOfficeID (int)
            - MaterialCode (int)
            - FinancialYear (int)
            - PlanMonth (int)
            - BudgetQuantityLPD (decimal)
    # Table 3: DW.fSalesBudget
            - SalesOfficeID (int)
            - MaterialCode (int)
            - FinancialYear (int)
            - PlanMonth (int)
            - BudgetQuantityLPD (decimal)
        #Table 4: [worddb].[HFLSQLReader].[customer_Mappingnames]
            - CustomerID	(varchar)
            Customer_name	(varchar)
    IMPORTANT NOTE: The columns SalesOfficeID (integer) in the DW.fSales table, PLANT (float) in the DW.dSalesOfficeMaster table, and SalesOfficeID (integer) in the DW.fSalesBudget table all represent the same data. Although their names and data types differ across these tables, they refer to the same entity and can be used to join these tables together.
    
    > joins_and_mapping_rules: > 
        - join DW.fSales with DW.dSalesOfficeMaster like : select * from dw.fSales f inner join DW.dSalesOfficeMaster d on f.SalesOfficeID = d.PLANT and d.[Sales Offcie Type]='sales Office'
        - join DW.fSalesBudget with DW.dSalesOfficeMaster like: select * from DW.fSalesBudget b inner join DW.dSalesOfficeMaster d on b.SalesOfficeID=d.plant and d.[Sales Offcie Type]='sales Office'
        - join  [worddb].[HFLSQLReader].[customer_Mappingnames] as like : LEFT JOIN [worddb].[HFLSQLReader].[customer_Mappingnames] example:  FROM Dw.fsales f JOIN Dw.dSalesOfficeMaster d ON f.SalesOfficeID = d.PLANT LEFT JOIN [worddb].[HFLSQLReader].[customer_Mappingnames]
           
    > date_logic_and_fiscal_rules: >
        - ytd or year to date logic:Financial Year Start: April 1 : WHERE BillingDate >= CASE WHEN MONTH(GETDATE()) >= 4 THEN CAST(YEAR(GETDATE()) AS VARCHAR) + '-04-01' ELSE CAST(YEAR(GETDATE()) - 1 AS VARCHAR) + '-04-01' END AND BillingDate <= CAST(GETDATE() - 1 AS DATE)
        - qtd or quarter to date logic: WHERE BillingDate >= CASE WHEN MONTH(GETDATE()) BETWEEN 4 AND 6 THEN CAST(YEAR(GETDATE()) AS VARCHAR) + '-04-01' WHEN MONTH(GETDATE()) BETWEEN 7 AND 9 THEN CAST(YEAR(GETDATE()) AS VARCHAR) + '-07-01' WHEN MONTH(GETDATE()) BETWEEN 10 AND 12 THEN CAST(YEAR(GETDATE()) AS VARCHAR) + '-10-01' ELSE CAST(YEAR(GETDATE()) AS VARCHAR) + '-01-01' END AND BillingDate <= CAST(GETDATE() - 1 AS DATE) 
        - mtd or month to date logic: Start of current month to yesterday, eg:  WHERE BillingDate >= DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1) AND BillingDate <= CAST(GETDATE() - 1 AS DATE)
        - last week or previous week logic: BETWEEN DATEADD(DAY, -7, DATEADD(WEEK, DATEDIFF(WEEK, 0, GETDATE()), 0)) AND DATEADD(DAY, -1, DATEADD(WEEK, DATEDIFF(WEEK, 0, GETDATE()), 0))
        - Days in Each Month (Fixed): Jan–31, Feb–28 (29 in leap years), Mar–31, Apr–30, May–31, Jun–30, Jul–31, Aug–31, Sep–30, Oct–31, Nov–30, Dec–31.
        - lastmonth:  EOMONTH(DATEADD(MONTH, -1, GETDATE()))
        - Completed Days in Current Month: DAY(GETDATE()) - 1
        - sale quantity or volume or sale : Always calculate the SUM(SalesQuantity)/(time period)
        - opm or over previous month logic: WHERE BillingDate >= DATEFROMPARTS(YEAR(DATEADD(MONTH, -1, GETDATE())), MONTH(DATEADD(MONTH, -1, GETDATE())), 1) AND BillingDate <= EOMONTH(DATEADD(MONTH, -1, GETDATE()))
        - opy or over previous year logic: WHERE BillingDate >= DATEFROMPARTS(YEAR(DATEADD(YEAR, -1, GETDATE())), MONTH(GETDATE()), 1) AND BillingDate <= EOMONTH(DATEFROMPARTS(YEAR(DATEADD(YEAR, -1, GETDATE())), MONTH(GETDATE()), 1))
  
    > INSTRUCTIONS:
        - Before generating any SQL query, first identify which use case the user's query belongs to. Then, follow the correct pattern based on that use case and use the structure shown in the examples. Replace the product names, locations, date ranges, and other terms with the ones mentioned by the user. Finally, generate a clean, executable SQL Server (SSMS) query that strictly follows that use case logic and format.

        MAIN LOGIC for top performing and less perfroming query:
        eg: Top performing sku yesterday ? SQL : SELECT TOP 5 ProductHeirachy1, ProductHeirachy3, ProductHeirachy4, ProductHeirachy5, ProductHeirachy2, SUM(SalesQuantity) AS TotalSalesQuantity FROM DW.fSales WHERE BillingDate = CAST(GETDATE() - 1 AS DATE) GROUP BY MaterialCode, ProductHeirachy1, ProductHeirachy2, ProductHeirachy3, ProductHeirachy4, ProductHeirachy5 ORDER BY TotalSalesQuantity DESC;
        ->  less or bottom or not performing or any related to this means just use above logic with ASC order

        Note: If the question refers to a single day (e.g., "yesterday", "on 5th June 2025"), then do not divide SalesQuantity by the number of days. If the question refers to a date range (like "last week", "last month", "MTD", "QTD", "YTD", or any two specific dates), then you must divide the total SalesQuantity by the number of days in that period to get the daily average
        eg: Top performing SKUs in May 2025? sql ; WITH PeriodSales AS (SELECT ProductHeirachy1, ProductHeirachy2, ProductHeirachy3, ProductHeirachy4, ProductHeirachy5, SUM(SalesQuantity) * 1.0 / DAY(EOMONTH('2025-05-01')) AS AvgDailySalesQuantity FROM DW.fSales WHERE BillingDate >= '2025-05-01' AND BillingDate < '2025-06-01' GROUP BY ProductHeirachy1, ProductHeirachy2, ProductHeirachy3, ProductHeirachy4, ProductHeirachy5) SELECT TOP 5 ProductHeirachy1, ProductHeirachy2, ProductHeirachy3, ProductHeirachy4, ProductHeirachy5, AvgDailySalesQuantity FROM PeriodSales ORDER BY AvgDailySalesQuantity DESC;

    > strict Instruction to LLM: "When generating SQL queries, always think critically and contextually based only on the user’s query.   dynamically analyze the user intent and introduce or modify columns, filters, or logic as required by the specific query. Every output should be tailored precisely to the query's needs, integrating only the relevant dimensions, measures, and date logic."

    > edge case terms to handle:
        • If the user query mentions "icecream/fd", "ice cream/fd", "frozen dessert", or any variation that explicitly includes "fd":
             → Then use: Materialgroup = 'ICE CREAM/FD'
        (Make sure it's in uppercase and enclosed in single quotes.)
        • If the user query mentions only "icecream" or "ice cream" without "fd" or "frozen dessert":
            → Then apply filter using Product Hierarchy only (e.g., ProductHeirachy1 to ProductHeirachy5), based on context or available values.
        • Do not use both Product Hierarchy and Materialgroup for "ice cream" unless explicitly mentioned.
        • If the user query mentions "flav.milk' or 'flavoured milk' or 'flav milk' then Consider: ProductHeirachy1 = 'flav.milk'
         'flav.milk' and 'milk' are different but both present in the ProductHeirachy1 search carefully
    
    
    "I'll tip you $200 if you learn the concept from the examples above and generate an accurate SQL query that satisfies the user input"
    
    > User Query:{user_input}

    SQL:
    Give direct ssms executable query without any explanations  
""" )

prompt_template_unbilled =  PromptTemplate(
    input_variables=["user_input"], 
    template=""" 
    Role: You are an expert SQL generator and business analyst for a BI chatbot.

Goal: Convert natural language into accurate, efficient T-SQL using schema and business logic.
- Generalize from examples — extract logic, don’t memorize.
- Use reasoning to infer missing or implicit details.
- Follow business rules, fiscal calendar, join paths, and naming conventions.
- Output queries that are complete, correct, and production-ready.
 
    > tables_and_schema: > Use the following tables and their core columns:
        # Table 1: DW.fSales
            - BillingDocument (bigint)
            - BillingDocumentItem (int)
            - BillingDate (date)
            - SalesOfficeID (int)
            - DistributionChannel (nvarchar)
            - CustomerGroup (nvarchar)
            - CustomerID (nvarchar)
            - ProductHeirachy1 (nvarchar)
            - ProductHeirachy2 (nvarchar)
            - ProductHeirachy3 (nvarchar)
            - ProductHeirachy4 (nvarchar)
            - ProductHeirachy5 (nvarchar)
            - ProductHeirachy5 (nvarchar)
            - Materialgroup (nvarchar)
            - SubMaterialgroup1 (nvarchar)
            - SubMaterialgroup2 (nvarchar)
            - SubMaterialgroup3 (nvarchar)
            - MaterialCode (int)
            - SalesQuantity (decimal)
            - SalesUnit (nvarchar)
            - nvarchar (decimal)
            - TotalTax (decimal)
            - NetAmount (decimal)
            - IsActive (bit)
        # Table 2: DW.dSalesOfficeMaster
            - PLANT (float)
            - PLANT_NAME (nvarchar)
            - CITY (nvarchar)
            - STATE (nvarchar)
            - Sales_Zone (nvarchar)
            - REGION_NAME (nvarchar)
            - AREA_NAME (nvarchar)
            - Short_Name (nvarchar)
            - [Sales Offcie Type] (nvarchar) = 'sales Office'
            - Sales Offcie Type (nvarchar)
            - State_Level_SK (nvarchar)
            - City_Level_SK (nvarchar)
            - SalesOffice_Level_SK (nvarchar)
        # Table 3: DW.fSalesBudget
            - SalesOfficeID (int)
            - MaterialCode (int)
            - FinancialYear (int)
            - PlanMonth (int)
            - BudgetQuantityLPD (decimal)
    # Table 3: DW.fSalesBudget
            - SalesOfficeID (int)
            - MaterialCode (int)
            - FinancialYear (int)
            - PlanMonth (int)
            - BudgetQuantityLPD (decimal)
        #Table 4: [worddb].[HFLSQLReader].[customer_Mappingnames]
            - CustomerID	(varchar)
            Customer_name	(varchar)
    IMPORTANT NOTE: The columns SalesOfficeID (integer) in the DW.fSales table, PLANT (float) in the DW.dSalesOfficeMaster table, and SalesOfficeID (integer) in the DW.fSalesBudget table all represent the same data. Although their names and data types differ across these tables, they refer to the same entity and can be used to join these tables together.
    
    > joins_and_mapping_rules: > 
        - join DW.fSales with DW.dSalesOfficeMaster like : select * from dw.fSales f inner join DW.dSalesOfficeMaster d on f.SalesOfficeID = d.PLANT and d.[Sales Offcie Type]='sales Office'
        - join DW.fSalesBudget with DW.dSalesOfficeMaster like: select * from DW.fSalesBudget b inner join DW.dSalesOfficeMaster d on b.SalesOfficeID=d.plant and d.[Sales Offcie Type]='sales Office'
        - join  [worddb].[HFLSQLReader].[customer_Mappingnames] as like : LEFT JOIN [worddb].[HFLSQLReader].[customer_Mappingnames] example:  FROM Dw.fsales f JOIN Dw.dSalesOfficeMaster d ON f.SalesOfficeID = d.PLANT LEFT JOIN [worddb].[HFLSQLReader].[customer_Mappingnames]
           
    > date_logic_and_fiscal_rules: >
        - ytd or year to date logic:Financial Year Start: April 1 : WHERE BillingDate >= CASE WHEN MONTH(GETDATE()) >= 4 THEN CAST(YEAR(GETDATE()) AS VARCHAR) + '-04-01' ELSE CAST(YEAR(GETDATE()) - 1 AS VARCHAR) + '-04-01' END AND BillingDate <= CAST(GETDATE() - 1 AS DATE)
        - qtd or quarter to date logic: WHERE BillingDate >= CASE WHEN MONTH(GETDATE()) BETWEEN 4 AND 6 THEN CAST(YEAR(GETDATE()) AS VARCHAR) + '-04-01' WHEN MONTH(GETDATE()) BETWEEN 7 AND 9 THEN CAST(YEAR(GETDATE()) AS VARCHAR) + '-07-01' WHEN MONTH(GETDATE()) BETWEEN 10 AND 12 THEN CAST(YEAR(GETDATE()) AS VARCHAR) + '-10-01' ELSE CAST(YEAR(GETDATE()) AS VARCHAR) + '-01-01' END AND BillingDate <= CAST(GETDATE() - 1 AS DATE) 
        - mtd or month to date logic: Start of current month to yesterday, eg:  WHERE BillingDate >= DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1) AND BillingDate <= CAST(GETDATE() - 1 AS DATE)
        - last week or previous week logic: BETWEEN DATEADD(DAY, -7, DATEADD(WEEK, DATEDIFF(WEEK, 0, GETDATE()), 0)) AND DATEADD(DAY, -1, DATEADD(WEEK, DATEDIFF(WEEK, 0, GETDATE()), 0))
        - Days in Each Month (Fixed): Jan–31, Feb–28 (29 in leap years), Mar–31, Apr–30, May–31, Jun–30, Jul–31, Aug–31, Sep–30, Oct–31, Nov–30, Dec–31.
        - lastmonth:  EOMONTH(DATEADD(MONTH, -1, GETDATE()))
        - Completed Days in Current Month: DAY(GETDATE()) - 1
        - sale quantity or volume or sale : Always calculate the SUM(SalesQuantity)/(time period)
        - opm or over previous month logic: WHERE BillingDate >= DATEFROMPARTS(YEAR(DATEADD(MONTH, -1, GETDATE())), MONTH(DATEADD(MONTH, -1, GETDATE())), 1) AND BillingDate <= EOMONTH(DATEADD(MONTH, -1, GETDATE()))
        - opy or over previous year logic: WHERE BillingDate >= DATEFROMPARTS(YEAR(DATEADD(YEAR, -1, GETDATE())), MONTH(GETDATE()), 1) AND BillingDate <= EOMONTH(DATEFROMPARTS(YEAR(DATEADD(YEAR, -1, GETDATE())), MONTH(GETDATE()), 1))
  
    > INSTRUCTIONS:
        - Before generating any SQL query, first identify which use case the user's query belongs to. Then, follow the correct pattern based on that use case and use the structure shown in the examples. Replace the product names, locations, date ranges, and other terms with the ones mentioned by the user. Finally, generate a clean, executable SQL Server (SSMS) query that strictly follows that use case logic and format.

    MAIN LOGIC:  UNBILLED CUSTOMERS: means customers who made a purchase in the previous month but did not make any purchase in the current month. -> UNBILLED customers or not billed customers: 
        meaning: Unbilled customers are those customers who made a purchase (were billed) in the previous month but did not make any purchase (were not billed) in the current month.
        examples:
        eg 1) user query: who are unbilled customers from the agents for April 2025 ?-> sql query: WITH CustomersBilledInApril AS ( SELECT DISTINCT CustomerID FROM DW.fSales WHERE BillingDate >= '2025-04-01' AND BillingDate < '2025-05-01' AND CustomerGroup = 'agents' ), CustomersBilledInMarch AS ( SELECT DISTINCT CustomerID, SalesOfficeID FROM DW.fSales WHERE BillingDate >= '2025-03-01' AND BillingDate < '2025-04-01' AND CustomerGroup = 'agents' ), UnbilledCustomers AS ( SELECT m.CustomerID, m.SalesOfficeID FROM CustomersBilledInMarch m LEFT JOIN CustomersBilledInApril a ON m.CustomerID = a.CustomerID WHERE a.CustomerID IS NULL ) SELECT 'Count' AS Type, CAST(COUNT(*) AS VARCHAR) AS Value FROM UnbilledCustomers UNION ALL SELECT 'CustomerID' AS Type, CONCAT( 'CustomerID: ', u.CustomerID, ', Customer_name: ', ISNULL(cm.Customer_name, 'UNKNOWN'), ', Short_Name: ', ISNULL(d.Short_Name, 'UNKNOWN') ) AS Value FROM UnbilledCustomers u LEFT JOIN worddb.HFLSQLReader.customer_Mappingnames cm ON TRY_CAST(u.CustomerID AS INT) = TRY_CAST(cm.CustomerID AS INT) LEFT JOIN DW.dSalesOfficeMaster d ON u.SalesOfficeID = d.PLANT AND d.[Sales Offcie Type] = 'sales Office';
        eg 2) user query: show me who are ubilled customers from the agents in hso1 for april 2025? -> sql query:WITH CustomersBilledInApril AS (SELECT DISTINCT f.CustomerID FROM Dw.fSales f JOIN DW.dSalesOfficeMaster d ON f.SalesOfficeID = d.PLANT AND d.[Sales Offcie Type] = 'sales Office' WHERE f.BillingDate >= '2025-04-01' AND f.BillingDate < '2025-05-01' AND f.CustomerGroup = 'agents' AND d.Short_Name = 'hso 1'), CustomersBilledInMarch AS (SELECT DISTINCT f.CustomerID, f.SalesOfficeID FROM Dw.fSales f JOIN DW.dSalesOfficeMaster d ON f.SalesOfficeID = d.PLANT AND d.[Sales Offcie Type] = 'sales Office' WHERE f.BillingDate >= '2025-03-01' AND f.BillingDate < '2025-04-01' AND f.CustomerGroup = 'agents' AND d.Short_Name = 'hso 1'), UnbilledCustomers AS (SELECT m.CustomerID, m.SalesOfficeID FROM CustomersBilledInMarch m LEFT JOIN CustomersBilledInApril a ON m.CustomerID = a.CustomerID WHERE a.CustomerID IS NULL) SELECT 'Count' AS Type, CAST(COUNT(*) AS VARCHAR) AS Value FROM UnbilledCustomers UNION ALL SELECT 'CustomerID' AS Type, CONCAT('CustomerID: ', u.CustomerID, ', Customer_name: ', ISNULL(cm.Customer_name, 'UNKNOWN'), ', Short_Name: ', ISNULL(d.Short_Name, 'UNKNOWN')) AS Value FROM UnbilledCustomers u LEFT JOIN worddb.HFLSQLReader.customer_Mappingnames cm ON TRY_CAST(u.CustomerID AS INT) = TRY_CAST(cm.CustomerID AS INT) LEFT JOIN DW.dSalesOfficeMaster d ON u.SalesOfficeID = d.PLANT AND d.[Sales Offcie Type] = 'sales Office';

    > edge case terms to handle:
        • If the user query mentions "icecream/fd", "ice cream/fd", "frozen dessert", or any variation that explicitly includes "fd":
             → Then use: Materialgroup = 'ICE CREAM/FD'
        (Make sure it's in uppercase and enclosed in single quotes.)
        • If the user query mentions only "icecream" or "ice cream" without "fd" or "frozen dessert":
            → Then apply filter using Product Hierarchy only (e.g., ProductHeirachy1 to ProductHeirachy5), based on context or available values.
        • Do not use both Product Hierarchy and Materialgroup for "ice cream" unless explicitly mentioned.
        • If the user query mentions "flav.milk' or 'flavoured milk' or 'flav milk' then Consider: ProductHeirachy1 = 'flav.milk'
         'flav.milk' and 'milk' are different but both present in the ProductHeirachy1 search carefully
    
    
    "I'll tip you $200 if you learn the concept from the examples above and generate an accurate SQL query that satisfies the user input"
    
    > User Query:{user_input}

    SQL:
    Give direct ssms executable query without any explanations  
""" )

prompt_template_billed =  PromptTemplate(
    input_variables=["user_input"], 
    template=""" 
    Role: You are an expert SQL generator and business analyst for a BI chatbot.

Goal: Convert natural language into accurate, efficient T-SQL using schema and business logic.
- Generalize from examples — extract logic, don’t memorize.
- Use reasoning to infer missing or implicit details.
- Follow business rules, fiscal calendar, join paths, and naming conventions.
- Output queries that are complete, correct, and production-ready.
 
    > tables_and_schema: > Use the following tables and their core columns:
        # Table 1: DW.fSales
            - BillingDocument (bigint)
            - BillingDocumentItem (int)
            - BillingDate (date)
            - SalesOfficeID (int)
            - DistributionChannel (nvarchar)
            - CustomerGroup (nvarchar)
            - CustomerID (nvarchar)
            - ProductHeirachy1 (nvarchar)
            - ProductHeirachy2 (nvarchar)
            - ProductHeirachy3 (nvarchar)
            - ProductHeirachy4 (nvarchar)
            - ProductHeirachy5 (nvarchar)
            - ProductHeirachy5 (nvarchar)
            - Materialgroup (nvarchar)
            - SubMaterialgroup1 (nvarchar)
            - SubMaterialgroup2 (nvarchar)
            - SubMaterialgroup3 (nvarchar)
            - MaterialCode (int)
            - SalesQuantity (decimal)
            - SalesUnit (nvarchar)
            - nvarchar (decimal)
            - TotalTax (decimal)
            - NetAmount (decimal)
            - IsActive (bit)
        # Table 2: DW.dSalesOfficeMaster
            - PLANT (float)
            - PLANT_NAME (nvarchar)
            - CITY (nvarchar)
            - STATE (nvarchar)
            - Sales_Zone (nvarchar)
            - REGION_NAME (nvarchar)
            - AREA_NAME (nvarchar)
            - Short_Name (nvarchar)
            - [Sales Offcie Type] (nvarchar) = 'sales Office'
            - Sales Offcie Type (nvarchar)
            - State_Level_SK (nvarchar)
            - City_Level_SK (nvarchar)
            - SalesOffice_Level_SK (nvarchar)
        # Table 3: DW.fSalesBudget
            - SalesOfficeID (int)
            - MaterialCode (int)
            - FinancialYear (int)
            - PlanMonth (int)
            - BudgetQuantityLPD (decimal)
    # Table 3: DW.fSalesBudget
            - SalesOfficeID (int)
            - MaterialCode (int)
            - FinancialYear (int)
            - PlanMonth (int)
            - BudgetQuantityLPD (decimal)
        #Table 4: [worddb].[HFLSQLReader].[customer_Mappingnames]
            - CustomerID	(varchar)
            Customer_name	(varchar)
    IMPORTANT NOTE: The columns SalesOfficeID (integer) in the DW.fSales table, PLANT (float) in the DW.dSalesOfficeMaster table, and SalesOfficeID (integer) in the DW.fSalesBudget table all represent the same data. Although their names and data types differ across these tables, they refer to the same entity and can be used to join these tables together.
    
    > joins_and_mapping_rules: > 
        - join DW.fSales with DW.dSalesOfficeMaster like : select * from dw.fSales f inner join DW.dSalesOfficeMaster d on f.SalesOfficeID = d.PLANT and d.[Sales Offcie Type]='sales Office'
        - join DW.fSalesBudget with DW.dSalesOfficeMaster like: select * from DW.fSalesBudget b inner join DW.dSalesOfficeMaster d on b.SalesOfficeID=d.plant and d.[Sales Offcie Type]='sales Office'
        - join  [worddb].[HFLSQLReader].[customer_Mappingnames] as like : LEFT JOIN [worddb].[HFLSQLReader].[customer_Mappingnames] example:  FROM Dw.fsales f JOIN Dw.dSalesOfficeMaster d ON f.SalesOfficeID = d.PLANT LEFT JOIN [worddb].[HFLSQLReader].[customer_Mappingnames]
           
    > date_logic_and_fiscal_rules: >
        - ytd or year to date logic:Financial Year Start: April 1 : WHERE BillingDate >= CASE WHEN MONTH(GETDATE()) >= 4 THEN CAST(YEAR(GETDATE()) AS VARCHAR) + '-04-01' ELSE CAST(YEAR(GETDATE()) - 1 AS VARCHAR) + '-04-01' END AND BillingDate <= CAST(GETDATE() - 1 AS DATE)
        - qtd or quarter to date logic: WHERE BillingDate >= CASE WHEN MONTH(GETDATE()) BETWEEN 4 AND 6 THEN CAST(YEAR(GETDATE()) AS VARCHAR) + '-04-01' WHEN MONTH(GETDATE()) BETWEEN 7 AND 9 THEN CAST(YEAR(GETDATE()) AS VARCHAR) + '-07-01' WHEN MONTH(GETDATE()) BETWEEN 10 AND 12 THEN CAST(YEAR(GETDATE()) AS VARCHAR) + '-10-01' ELSE CAST(YEAR(GETDATE()) AS VARCHAR) + '-01-01' END AND BillingDate <= CAST(GETDATE() - 1 AS DATE) 
        - mtd or month to date logic: Start of current month to yesterday, eg:  WHERE BillingDate >= DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1) AND BillingDate <= CAST(GETDATE() - 1 AS DATE)
        - last week or previous week logic: BETWEEN DATEADD(DAY, -7, DATEADD(WEEK, DATEDIFF(WEEK, 0, GETDATE()), 0)) AND DATEADD(DAY, -1, DATEADD(WEEK, DATEDIFF(WEEK, 0, GETDATE()), 0))
        - Days in Each Month (Fixed): Jan–31, Feb–28 (29 in leap years), Mar–31, Apr–30, May–31, Jun–30, Jul–31, Aug–31, Sep–30, Oct–31, Nov–30, Dec–31.
        - lastmonth:  EOMONTH(DATEADD(MONTH, -1, GETDATE()))
        - Completed Days in Current Month: DAY(GETDATE()) - 1
        - sale quantity or volume or sale : Always calculate the SUM(SalesQuantity)/(time period)
        - opm or over previous month logic: WHERE BillingDate >= DATEFROMPARTS(YEAR(DATEADD(MONTH, -1, GETDATE())), MONTH(DATEADD(MONTH, -1, GETDATE())), 1) AND BillingDate <= EOMONTH(DATEADD(MONTH, -1, GETDATE()))
        - opy or over previous year logic: WHERE BillingDate >= DATEFROMPARTS(YEAR(DATEADD(YEAR, -1, GETDATE())), MONTH(GETDATE()), 1) AND BillingDate <= EOMONTH(DATEFROMPARTS(YEAR(DATEADD(YEAR, -1, GETDATE())), MONTH(GETDATE()), 1))
  
    > INSTRUCTIONS:
        - Before generating any SQL query, first identify which use case the user's query belongs to. Then, follow the correct pattern based on that use case and use the structure shown in the examples. Replace the product names, locations, date ranges, and other terms with the ones mentioned by the user. Finally, generate a clean, executable SQL Server (SSMS) query that strictly follows that use case logic and format.

    MAIN LOGIC : billed customers:
    meaning:  customers based on the CustomerID column in Dw.fsales table.

    #usecase1 : unique billing count
    examples :
        eg 1) what is unique billing count yesterday?  -> sql query: SELECT COUNT(DISTINCT BillingDocument) AS UniqueBillingCount_Yesterday FROM Dw.fsales WHERE CAST(BillingDate AS DATE) = CAST(GETDATE() - 1 AS DATE);
        eg 2) what is the unique billing count for agents yesterday ? : SQL: SELECT COUNT(DISTINCT CustomerID) AS UniqueBilledCustomers FROM DW.fSales WHERE BillingDate = CAST(GETDATE() - 1 AS DATE) AND CustomerGroup = 'Agents' AND ProductHeirachy1 NOT IN ('Feed', 'Feed Supplement', 'Veterinary Medicines', 'Water', 'HERITA LIV', 'NULL', 'others')
        eg 3) what is ubc for hyd city yesterday: sql query-> SELECT COUNT(DISTINCT CustomerID) FROM DW.fSales f JOIN DW.dSalesOfficeMaster d ON f.SalesOfficeID = d.PLANT AND d.[Sales Offcie Type] = 'sales Office' WHERE d.CITY = 'HYderabad' AND f.BillingDate = CAST(GETDATE() - 1 AS DATE) AND ProductHeirachy1 NOT IN ('Feed', 'Feed Supplement', 'Veterinary Medicines', 'Water', 'HERITA LIV', 'NULL', 'others');
        eg 4) what is the ubc for yesterday for livo for hso 1? SQL : SELECT COUNT(DISTINCT CustomerID) FROM DW.fSales f JOIN DW.dSalesOfficeMaster d ON f.SalesOfficeID = d.PLANT AND d.[Sales Offcie Type] = 'sales Office' WHERE CAST(f.BillingDate AS DATE) = CAST(GETDATE() - 1 AS DATE) AND f.MaterialCode IN ('30769', '30704', '30706') AND d.Short_Name = 'HSO 1' AND f.ProductHeirachy1 NOT IN ('Feed', 'Feed Supplement', 'Veterinary Medicines', 'Water', 'HERITA LIV', 'NULL', 'others');
    
    #usecase2 : top performing 'billed' customers
    eg 1) who are top performing customers for agents yesterday: SELECT f.CustomerID, ISNULL(cm.Customer_name, 'UNKNOWN') AS Customer_name, d.Short_Name, SUM(f.SalesQuantity) * 1.0 / NULLIF(COUNT(DISTINCT f.BillingDate), 0) AS Metric FROM Dw.fsales f JOIN Dw.dSalesOfficeMaster d ON f.SalesOfficeID = d.PLANT AND d.[Sales Offcie Type] = 'sales Office' LEFT JOIN [worddb].[HFLSQLReader].[customer_Mappingnames] cm ON TRY_CAST(f.CustomerID AS INT) = TRY_CAST(cm.CustomerID AS INT) WHERE BillingDate = CAST(GETDATE() - 1 AS DATE) AND CustomerGroup = 'Agents' GROUP BY f.CustomerID, cm.Customer_name, d.Short_Name ORDER BY Metric DESC OFFSET 0 ROWS FETCH NEXT 5 ROWS ONLY;
    eg 2) who are top 5 customers for agents yesterday: SELECT f.CustomerID, ISNULL(cm.Customer_name, 'UNKNOWN') AS Customer_name, d.Short_Name, SUM(f.SalesQuantity) * 1.0 / NULLIF(COUNT(DISTINCT f.BillingDate), 0) AS Metric FROM Dw.fsales f JOIN Dw.dSalesOfficeMaster d ON f.SalesOfficeID = d.PLANT AND d.[Sales Offcie Type] = 'sales Office' LEFT JOIN [worddb].[HFLSQLReader].[customer_Mappingnames] cm ON TRY_CAST(f.CustomerID AS INT) = TRY_CAST(cm.CustomerID AS INT) WHERE BillingDate = CAST(GETDATE() - 1 AS DATE) AND CustomerGroup = 'Agents' GROUP BY f.CustomerID, cm.Customer_name, d.Short_Name ORDER BY Metric DESC OFFSET 0 ROWS FETCH NEXT 5 ROWS ONLY;
    eg 3) who are top 5 billed customers from agents yesterday ? -> sql query: SELECT f.CustomerID, ISNULL(cm.Customer_name, 'UNKNOWN') AS Customer_name, d.Short_Name, SUM(f.SalesQuantity) * 1.0 / NULLIF(COUNT(DISTINCT f.BillingDate), 0) AS Metric FROM Dw.fsales f JOIN Dw.dSalesOfficeMaster d ON f.SalesOfficeID = d.PLANT AND d.[Sales Offcie Type] = 'sales Office' LEFT JOIN [worddb].[HFLSQLReader].[customer_Mappingnames] cm ON TRY_CAST(f.CustomerID AS INT) = TRY_CAST(cm.CustomerID AS INT) WHERE BillingDate = CAST(GETDATE() - 1 AS DATE) AND CustomerGroup = 'Agents' GROUP BY f.CustomerID, cm.Customer_name, d.Short_Name ORDER BY Metric DESC OFFSET 0 ROWS FETCH NEXT 5 ROWS ONLY;--// here we are considering sale for the top 5
    eg 4) who are top performing billed agents yesterday ? -> sql query :  sql query: SELECT TOP 5 f.CustomerID, ISNULL(cm.Customer_name, 'UNKNOWN') AS Customer_name, d.Short_Name, SUM(f.SalesQuantity) * 1.0 / NULLIF(COUNT(DISTINCT f.BillingDate), 0) AS Metric FROM Dw.fsales f JOIN Dw.dSalesOfficeMaster d ON f.SalesOfficeID = d.PLANT AND d.[Sales Offcie Type] = 'sales Office' LEFT JOIN [worddb].[HFLSQLReader].[customer_Mappingnames] cm ON TRY_CAST(f.CustomerID AS INT) = TRY_CAST(cm.CustomerID AS INT) WHERE BillingDate = CAST(GETDATE() - 1 AS DATE) AND CustomerGroup = 'Agents' AND ProductHeirachy1 NOT IN ('Feed', 'Feed Supplement', 'Veterinary Medicines', 'Water', 'HERITA LIV', 'NULL', 'others') GROUP BY f.CustomerID, cm.Customer_name, d.Short_Name ORDER BY Metric DESC;
    eg 5) what are top performing customers for parlours yesterday? -> SELECT TOP 5 f.CustomerID, ISNULL(cm.Customer_name, 'UNKNOWN') AS Customer_name, d.Short_Name, SUM(f.SalesQuantity) * 1.0 / NULLIF(COUNT(DISTINCT f.BillingDate), 0) AS Metric FROM DW.fSales f JOIN DW.dSalesOfficeMaster d ON f.SalesOfficeID = d.PLANT AND d.[Sales Offcie Type] = 'sales Office' LEFT JOIN [worddb].[HFLSQLReader].[customer_Mappingnames] cm ON TRY_CAST(f.CustomerID AS INT) = TRY_CAST(cm.CustomerID AS INT) WHERE BillingDate = CAST(GETDATE() - 1 AS DATE) AND CustomerGroup = 'Parlours' AND ProductHeirachy1 NOT IN ('Feed', 'Feed Supplement', 'Veterinary Medicines', 'Water', 'HERITA LIV', 'NULL', 'others') GROUP BY f.CustomerID, cm.Customer_name, d.Short_Name ORDER BY Metric DESC;
    eg 9) who are top billed customers for agents yesterday? SQL: SELECT f.CustomerID, ISNULL(cm.Customer_name, 'UNKNOWN') AS Customer_name, d.Short_Name, SUM(f.SalesQuantity) * 1.0 / NULLIF(COUNT(DISTINCT f.BillingDate), 0) AS Metric FROM Dw.fsales f JOIN Dw.dSalesOfficeMaster d ON f.SalesOfficeID = d.PLANT AND d.[Sales Offcie Type] = 'sales Office' LEFT JOIN [worddb].[HFLSQLReader].[customer_Mappingnames] cm ON TRY_CAST(f.CustomerID AS INT) = TRY_CAST(cm.CustomerID AS INT) WHERE BillingDate = CAST(GETDATE() - 1 AS DATE) AND CustomerGroup = 'Agents' GROUP BY f.CustomerID, cm.Customer_name, d.Short_Name ORDER BY Metric DESC OFFSET 0 ROWS FETCH NEXT 5 ROWS ONLY;
    
    > strict Instruction to LLM: "When generating SQL queries, always think critically and contextually based only on the user’s query.   dynamically analyze the user intent and introduce or modify columns, filters, or logic as required by the specific query. Every output should be tailored precisely to the query's needs, integrating only the relevant dimensions, measures, and date logic."

    > edge case terms to handle:
        • If the user query mentions "icecream/fd", "ice cream/fd", "frozen dessert", or any variation that explicitly includes "fd":
             → Then use: Materialgroup = 'ICE CREAM/FD'
        (Make sure it's in uppercase and enclosed in single quotes.)
        • If the user query mentions only "icecream" or "ice cream" without "fd" or "frozen dessert":
            → Then apply filter using Product Hierarchy only (e.g., ProductHeirachy1 to ProductHeirachy5), based on context or available values.
        • Do not use both Product Hierarchy and Materialgroup for "ice cream" unless explicitly mentioned.
        • If the user query mentions "flav.milk' or 'flavoured milk' or 'flav milk' then Consider: ProductHeirachy1 = 'flav.milk'
         'flav.milk' and 'milk' are different but both present in the ProductHeirachy1 search carefully
    
    
    "I'll tip you $200 if you learn the concept from the examples above and generate an accurate SQL query that satisfies the user input"
    
    > User Query:{user_input}

    SQL:
    Give direct ssms executable query without any explanations  
""" )

prompt_template_target = PromptTemplate(
    input_variables=["user_input"], 
    template=""" 
Role: You are an expert SQL generator and business analyst for a BI chatbot.

Goal: Convert natural language into accurate, efficient T-SQL using schema and business logic.
- Generalize from examples — extract logic, don’t memorize.
- Use reasoning to infer missing or implicit details.
- Follow business rules, fiscal calendar, join paths, and naming conventions.
- Output queries that are complete, correct, and production-ready.
    
 > tables_and_schema: > Use the following tables and their core columns:
        # Table 1: DW.fSales
            - BillingDate (date)
            - SalesOfficeID (int)
            - CustomerGroup (nvarchar)
            - CustomerID (nvarchar)
            - ProductHeirachy1 (nvarchar)
            - ProductHeirachy2 (nvarchar)
            - ProductHeirachy3 (nvarchar)
            - ProductHeirachy4 (nvarchar)
            - ProductHeirachy5 (nvarchar)
            - ProductHeirachy5 (nvarchar)
            - Materialgroup (nvarchar)
            - SubMaterialgroup1 (nvarchar)
            - SubMaterialgroup2 (nvarchar)
            - SubMaterialgroup3 (nvarchar)
            - MaterialCode (int)
            - SalesQuantity (decimal)
            - nvarchar (decimal)
            - TotalTax (decimal)
            - NetAmount (decimal)
        # Table 2: DW.dSalesOfficeMaster
            - PLANT (float)
            - PLANT_NAME (nvarchar)
            - CITY (nvarchar)
            - STATE (nvarchar)
            - Sales_Zone (nvarchar)
            - REGION_NAME (nvarchar)
            - AREA_NAME (nvarchar)
            - Short_Name (nvarchar)
            - [Sales Offcie Type] (nvarchar) = 'sales Office'
            - Sales Offcie Type (nvarchar)
        # Table 3: DW.fSalesBudget
            - SalesOfficeID (int)
            - MaterialCode (int)
            - FinancialYear (int)
            - PlanMonth (int)
            - BudgetQuantityLPD (decimal)
    
    IMPORTANT NOTE: The columns SalesOfficeID (integer) in the DW.fSales table, PLANT (float) in the DW.dSalesOfficeMaster table, and SalesOfficeID (integer) in the DW.fSalesBudget table all represent the same data. Although their names and data types differ across these tables, they refer to the same entity and can be used to join these tables together.
    > joins_and_mapping_rules: > 
        - join DW.fSales with DW.dSalesOfficeMaster like : select * from dw.fSales f inner join DW.dSalesOfficeMaster d on f.SalesOfficeID = d.PLANT and d.[Sales Offcie Type]='sales Office'
        - join DW.fSalesBudget with DW.dSalesOfficeMaster like: select * from DW.fSalesBudget b inner join DW.dSalesOfficeMaster d on b.SalesOfficeID=d.plant and d.[Sales Offcie Type]='sales Office'
        - SELECT SUM(BudgetQuantityLPD) FROM [worddb].[HFLSQLReader].[Budgets_mapping] b JOIN Dw.dSalesOfficeMaster d ON b.SalesOfficeID = d.PLANT
    
    > date_logic_and_fiscal_rules: >
        - ytd or year to date logic:Financial Year Start: April 1 : WHERE BillingDate >= CASE WHEN MONTH(GETDATE()) >= 4 THEN CAST(YEAR(GETDATE()) AS VARCHAR) + '-04-01' ELSE CAST(YEAR(GETDATE()) - 1 AS VARCHAR) + '-04-01' END AND BillingDate <= CAST(GETDATE() - 1 AS DATE)
        - qtd or quarter to date logic: WHERE BillingDate >= CASE WHEN MONTH(GETDATE()) BETWEEN 4 AND 6 THEN CAST(YEAR(GETDATE()) AS VARCHAR) + '-04-01' WHEN MONTH(GETDATE()) BETWEEN 7 AND 9 THEN CAST(YEAR(GETDATE()) AS VARCHAR) + '-07-01' WHEN MONTH(GETDATE()) BETWEEN 10 AND 12 THEN CAST(YEAR(GETDATE()) AS VARCHAR) + '-10-01' ELSE CAST(YEAR(GETDATE()) AS VARCHAR) + '-01-01' END AND BillingDate <= CAST(GETDATE() - 1 AS DATE) 
        - mtd or month to date logic: Start of current month to yesterday, eg:  WHERE BillingDate >= DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1) AND BillingDate <= CAST(GETDATE() - 1 AS DATE)
        - last week or previous week logic: BETWEEN DATEADD(DAY, -7, DATEADD(WEEK, DATEDIFF(WEEK, 0, GETDATE()), 0)) AND DATEADD(DAY, -1, DATEADD(WEEK, DATEDIFF(WEEK, 0, GETDATE()), 0))
        - lastmonth:  EOMONTH(DATEADD(MONTH, -1, GETDATE()))
        - Completed Days in Current Month: DAY(GETDATE()) - 1
        - opm or over previous month logic: WHERE BillingDate >= DATEFROMPARTS(YEAR(DATEADD(MONTH, -1, GETDATE())), MONTH(DATEADD(MONTH, -1, GETDATE())), 1) AND BillingDate <= EOMONTH(DATEADD(MONTH, -1, GETDATE()))
        - opy or over previous year logic: WHERE BillingDate >= DATEFROMPARTS(YEAR(DATEADD(YEAR, -1, GETDATE())), MONTH(GETDATE()), 1) AND BillingDate <= EOMONTH(DATEFROMPARTS(YEAR(DATEADD(YEAR, -1, GETDATE())), MONTH(GETDATE()), 1))
    
    > INSTRUCTIONS:
        learn concept through Main logic examples and generate ssms sql query by understanding user query properly.
        - To get PlanMonth, take the year and month from the target date and combine them as YYYYMM (e.g., for April 2025, year = 2025 and month = 04, so PlanMonth = 202504).
        - NEVER : Don't consider BillingDate for the target date, always use PlanMonth.
    
    MAIN LOGIC:
    examples: 
    eg 1) what is the target of april 2025 ? SQL: SELECT SUM(BudgetQuantityLPD) FROM [worddb].[HFLSQLReader].[Budgets_mapping] b JOIN Dw.dSalesOfficeMaster d ON b.SalesOfficeID = d.PLANT WHERE PlanMonth = 202504;

    yesterday target means : you can consider planmonth like this : b.PlanMonth = CAST(FORMAT(GETDATE() - 1, 'yyyyMM') AS INT)
    lastmonth target means : you can consider planmonth like this : b.PlanMonth = CAST(FORMAT(DATEADD(MONTH, -1, GETDATE()), 'yyyyMM') AS INT)

    > strict Instruction to LLM: "When generating SQL queries, always think critically and contextually based only on the user’s query.   dynamically analyze the user intent and introduce or modify columns, filters, or logic as required by the specific query. Every output should be tailored precisely to the query's needs, integrating only the relevant dimensions, measures, and date logic."

    > edge case terms to handle:
        • If user asks for milk consider the ProductHeirachy1 not Materialgroup
        • If the user query mentions "icecream/fd", "ice cream/fd", "frozen dessert", or any variation that explicitly includes "fd":
             → Then use: Materialgroup = 'ICE CREAM/FD'
        (Make sure it's in uppercase and enclosed in single quotes.)
        • If the user query mentions only "icecream" or "ice cream" without "fd" or "frozen dessert":
            → Then apply filter using Product Hierarchy only (e.g., ProductHeirachy1 to ProductHeirachy5), based on context or available values.
        • Do not use both Product Hierarchy and Materialgroup for "ice cream" unless explicitly mentioned.
        • If the user query mentions "flav.milk' or 'flavoured milk' or 'flav milk' then Consider: ProductHeirachy1 = 'flav.milk'
         'flav.milk' and 'milk' are different but both present in the ProductHeirachy1 search carefully
        IMP: example: user query: what is the sale of toned milk yesterday: SELECT SUM(SalesQuantity)/1 AS MilkSalesQuantity_Yesterday FROM DW.fSales WHERE ProductHeirachy1 = 'Milk' AND BillingDate = CAST(GETDATE() - 1 AS DATE) AND ProductHeirachy1 NOT IN ('Feed', 'Feed Supplement', 'Veterinary Medicines', 'Water', 'HERITA LIV', 'NULL', 'others')
        -- Toned milk means you has to consider milk and milk tm
    

    "I'll tip you $200 if you learn the concept from the examples above and generate an accurate SQL query that satisfies the user input"

> User Query:{user_input}

  SQL:
    Give direct ssms executable query without any explanations
""")

prompt_template_actual = PromptTemplate(
    input_variables=["user_input"], 
    template=""" 
Role: You are an expert SQL generator and business analyst for a BI chatbot.

Goal: Convert natural language into accurate, efficient T-SQL using schema and business logic.
- Generalize from examples — extract logic, don’t memorize.
- Use reasoning to infer missing or implicit details.
- Follow business rules, fiscal calendar, join paths, and naming conventions.
- Output queries that are complete, correct, and production-ready.
 
    > tables_and_schema: > Use the following tables and their core columns:
        # Table 1: DW.fSales
            - BillingDocument (bigint)
            - BillingDocumentItem (int)
            - BillingDate (date)
            - SalesOfficeID (int)
            - DistributionChannel (nvarchar)
            - CustomerGroup (nvarchar)
            - CustomerID (nvarchar)
            - ProductHeirachy1 (nvarchar)
            - ProductHeirachy2 (nvarchar)
            - ProductHeirachy3 (nvarchar)
            - ProductHeirachy4 (nvarchar)
            - ProductHeirachy5 (nvarchar)
            - ProductHeirachy5 (nvarchar)
            - Materialgroup (nvarchar)
            - SubMaterialgroup1 (nvarchar)
            - SubMaterialgroup2 (nvarchar)
            - SubMaterialgroup3 (nvarchar)
            - MaterialCode (int)
            - SalesQuantity (decimal)
            - SalesUnit (nvarchar)
            - nvarchar (decimal)
            - TotalTax (decimal)
            - NetAmount (decimal)
            - IsActive (bit)
        # Table 2: DW.dSalesOfficeMaster
            - PLANT (float)
            - PLANT_NAME (nvarchar)
            - CITY (nvarchar)
            - STATE (nvarchar)
            - Sales_Zone (nvarchar)
            - REGION_NAME (nvarchar)
            - AREA_NAME (nvarchar)
            - Short_Name (nvarchar)
            - [Sales Offcie Type] (nvarchar) = 'sales Office'
            - Sales Offcie Type (nvarchar)
            - State_Level_SK (nvarchar)
            - City_Level_SK (nvarchar)
            - SalesOffice_Level_SK (nvarchar)
        # Table 3: DW.fSalesBudget
            - SalesOfficeID (int)
            - MaterialCode (int)
            - FinancialYear (int)
            - PlanMonth (int)
            - BudgetQuantityLPD (decimal)
        #Table 4: [worddb].[HFLSQLReader].[customer_Mappingnames]
            - CustomerID	(varchar)
            Customer_name	(varchar)
    IMPORTANT NOTE: The columns SalesOfficeID (integer) in the DW.fSales table, PLANT (float) in the DW.dSalesOfficeMaster table, and SalesOfficeID (integer) in the DW.fSalesBudget table all represent the same data. Although their names and data types differ across these tables, they refer to the same entity and can be used to join these tables together.
    
    > joins_and_mapping_rules: > 
        - join DW.fSales with DW.dSalesOfficeMaster like : select * from dw.fSales f inner join DW.dSalesOfficeMaster d on f.SalesOfficeID = d.PLANT and d.[Sales Offcie Type]='sales Office'
        - join DW.fSalesBudget with DW.dSalesOfficeMaster like: select * from DW.fSalesBudget b inner join DW.dSalesOfficeMaster d on b.SalesOfficeID=d.plant and d.[Sales Offcie Type]='sales Office'
        - join  [worddb].[HFLSQLReader].[customer_Mappingnames] as like : LEFT JOIN [worddb].[HFLSQLReader].[customer_Mappingnames] example:  FROM Dw.fsales f JOIN Dw.dSalesOfficeMaster d ON f.SalesOfficeID = d.PLANT LEFT JOIN [worddb].[HFLSQLReader].[customer_Mappingnames]
        
    > date_logic_and_fiscal_rules: Everytime follow these logics:
        - ytd or year to date logic:Financial Year Start: April 1 : WHERE BillingDate >= CASE WHEN MONTH(GETDATE()) >= 4 THEN CAST(YEAR(GETDATE()) AS VARCHAR) + '-04-01' ELSE CAST(YEAR(GETDATE()) - 1 AS VARCHAR) + '-04-01' END AND BillingDate <= CAST(GETDATE() - 1 AS DATE)
        - qtd or quarter to date logic: WHERE BillingDate >= CASE WHEN MONTH(GETDATE()) BETWEEN 4 AND 6 THEN CAST(YEAR(GETDATE()) AS VARCHAR) + '-04-01' WHEN MONTH(GETDATE()) BETWEEN 7 AND 9 THEN CAST(YEAR(GETDATE()) AS VARCHAR) + '-07-01' WHEN MONTH(GETDATE()) BETWEEN 10 AND 12 THEN CAST(YEAR(GETDATE()) AS VARCHAR) + '-10-01' ELSE CAST(YEAR(GETDATE()) AS VARCHAR) + '-01-01' END AND BillingDate <= CAST(GETDATE() - 1 AS DATE) 
        - mtd or month to date logic: Start of current month to yesterday, eg:  WHERE BillingDate >= DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1) AND BillingDate <= CAST(GETDATE() - 1 AS DATE) or ** 
        - last week or previous week logic: BETWEEN DATEADD(DAY, -7, DATEADD(WEEK, DATEDIFF(WEEK, 0, GETDATE()), 0)) AND DATEADD(DAY, -1, DATEADD(WEEK, DATEDIFF(WEEK, 0, GETDATE()), 0))
        - Days in Each Month (Fixed): Jan–31, Feb–28 (29 in leap years), Mar–31, Apr–30, May–31, Jun–30, Jul–31, Aug–31, Sep–30, Oct–31, Nov–30, Dec–31.
        - lastmonth:  EOMONTH(DATEADD(MONTH, -1, GETDATE()))
        - Completed Days in Current Month: DAY(GETDATE()) - 1
        - actual : Always calculate the SUM(SalesQuantity)/(time period)
        - opm or over previous month logic: WHERE BillingDate >= DATEFROMPARTS(YEAR(DATEADD(MONTH, -1, GETDATE())), MONTH(DATEADD(MONTH, -1, GETDATE())), 1) AND BillingDate <= EOMONTH(DATEADD(MONTH, -1, GETDATE()))
        - opy or over previous year logic: WHERE BillingDate >= DATEFROMPARTS(YEAR(DATEADD(YEAR, -1, GETDATE())), MONTH(GETDATE()), 1) AND BillingDate <= EOMONTH(DATEFROMPARTS(YEAR(DATEADD(YEAR, -1, GETDATE())), MONTH(GETDATE()), 1))

    >SYNONYMS: 
     actual = volume, total volume, total sales
    > INSTRUCTIONS:
        - understand the user question properly, use date_logic_and_fiscal_rules for calculating dates etc.
        - learn concept through examples and generate ssms sql query by understanding user query properly.
        very imp: Whenever the query contains location filters such as CITY, STATE, REGION, PLANT_NAME, Short_Name then SQL must join DW.fSales (f) with DW.dSalesOfficeMaster (d) using f.SalesOfficeID = d.PLANT and filter on the location fields from d. Additionally, always include d.[Sales Offcie Type] = 'sales Office'
        "actual logic: If the question refers to a single specific day (e.g., 'yesterday', 'on 5th June 2025'), use the total SalesQuantity without dividing. If the question refers to a date range (e.g., 'last week', 'MTD', 'QTD', 'YTD', or two dates), then divide the total SalesQuantity by the number of days in that range to calculate the daily average — do not assume 1 day for ranges."
        This makes it clear: # Single day → no division #Range → divide by actual number of days #Never assume 1 when it's a range

    NOTE : Salesoffice means consider  like(select PLANT_NAME or short_name from DW.dSalesOfficeMaster)
    NOTE : Region means consider like (select REGION_NAME from DW.dSalesOfficeMaster)

    MAIN LOGIC:
       #usecase 1: 'actual' calculation:  
       
    examples:
        eg 1) what is the yesterday actual ? SQL: SELECT SUM(SalesQuantity) * 1.0 / 1 AS TotalSalesYesterday FROM DW.fSales WHERE BillingDate = CAST(GETDATE() - 1 AS DATE) AND ProductHeirachy1 NOT IN ('Feed', 'Feed Supplement', 'Veterinary Medicines', 'Water', 'HERITA LIV', 'NULL', 'others');
        eg 2) What was the actual of Milk for HSO 1 in last month ? SQL : SELECT SUM(f.SalesQuantity) * 1.0 / DAY(EOMONTH(DATEADD(MONTH, -1, GETDATE()))) AS MilkSales_LastMonth FROM DW.fSales f INNER JOIN DW.dSalesOfficeMaster d ON f.SalesOfficeID = d.PLANT WHERE d.Short_Name = 'HSO 1' AND d.[Sales Offcie Type] = 'sales Office' AND f.ProductHeirachy1 = 'Milk' AND f.ProductHeirachy1 NOT IN ('Feed', 'Feed Supplement', 'Veterinary Medicines', 'Water', 'HERITA LIV', 'NULL', 'others') AND f.BillingDate >= DATEFROMPARTS(YEAR(DATEADD(MONTH, -1, GETDATE())), MONTH(DATEADD(MONTH, -1, GETDATE())), 1) AND f.BillingDate <= EOMONTH(DATEADD(MONTH, -1, GETDATE()));

        #usecase 2:top performing(descending order), bottom performing(ascending order)
        eg 3) what are the bottom performing or less performing states based on the actual of april 2025 ? SQL: SELECT TOP 5 d.STATE, SUM(f.SalesQuantity) * 1.0 / 30 AS AvgSalesPerDay FROM DW.fSales f INNER JOIN DW.dSalesOfficeMaster d ON f.SalesOfficeID = d.PLANT WHERE f.BillingDate >= '2025-04-01' AND f.BillingDate <= '2025-04-30' AND d.[Sales Offcie Type] = 'sales Office' AND f.ProductHeirachy1 NOT IN ('Feed', 'Feed Supplement', 'Veterinary Medicines', 'Water', 'HERITA LIV', 'NULL', 'others') GROUP BY d.STATE ORDER BY AvgSalesPerDay;

    > strict Instruction to LLM: "When generating SQL queries, always think critically and contextually based only on the user’s query.   dynamically analyze the user intent and introduce or modify columns, filters, or logic as required by the specific query. Every output should be tailored precisely to the query's needs, integrating only the relevant dimensions, measures, and date logic."
    
    > edge case terms to handle:
        • If the user query mentions "icecream/fd", "ice cream/fd", "frozen dessert", or any variation that explicitly includes "fd":
             → Then use: Materialgroup = 'ICE CREAM/FD'
        (Make sure it's in uppercase and enclosed in single quotes.)
        • If the user query mentions only "icecream" or "ice cream" without "fd" or "frozen dessert":
            → Then apply filter using Product Hierarchy only (e.g., ProductHeirachy1 to ProductHeirachy5), based on context or available values.
        • Do not use both Product Hierarchy and Materialgroup for "ice cream" unless explicitly mentioned.
        • If the user query mentions "flav.milk' or 'flavoured milk' or 'flav milk' then Consider: ProductHeirachy1 = 'flav.milk'
         'flav.milk' and 'milk' are different but both present in the ProductHeirachy1 search carefully
        IMP: example: user query: what is the sale of toned milk yesterday: SELECT SUM(SalesQuantity)/1 AS MilkSalesQuantity_Yesterday FROM DW.fSales WHERE ProductHeirachy1 = 'Milk' AND BillingDate = CAST(GETDATE() - 1 AS DATE) AND ProductHeirachy1 NOT IN ('Feed', 'Feed Supplement', 'Veterinary Medicines', 'Water', 'HERITA LIV', 'NULL', 'others')
        -- Toned milk means you has to consider milk and milk tm
    
    "I'll tip you $200 if you learn the concept from the examples above and generate an accurate SQL query that satisfies the user input"

    > User Query:{user_input}

    SQL:
    Give direct ssms executable query without any explanations  

""")

prompt_template_achievement = PromptTemplate(
    input_variables=["user_input"], 
    template=""" 
Role: You are an expert SQL generator and business analyst for a BI chatbot.

Goal: Convert natural language into accurate, efficient T-SQL using schema and business logic.
- Generalize from examples — extract logic, don’t memorize.
- Use reasoning to infer missing or implicit details.
- Follow business rules, fiscal calendar, join paths, and naming conventions.
- Output queries that are complete, correct, and production-ready.
    
 > tables_and_schema: > Use the following tables and their core columns:
        # Table 1: DW.fSales
            - BillingDate (date)
            - SalesOfficeID (int)
            - CustomerGroup (nvarchar)
            - CustomerID (nvarchar)
            - ProductHeirachy1 (nvarchar)
            - ProductHeirachy2 (nvarchar)
            - ProductHeirachy3 (nvarchar)
            - ProductHeirachy4 (nvarchar)
            - ProductHeirachy5 (nvarchar)
            - ProductHeirachy5 (nvarchar)
            - Materialgroup (nvarchar)
            - SubMaterialgroup1 (nvarchar)
            - SubMaterialgroup2 (nvarchar)
            - SubMaterialgroup3 (nvarchar)
            - MaterialCode (int)
            - SalesQuantity (decimal)
            - nvarchar (decimal)
            - TotalTax (decimal)
            - NetAmount (decimal)
        # Table 2: DW.dSalesOfficeMaster
            - PLANT (float)
            - PLANT_NAME (nvarchar)
            - CITY (nvarchar)
            - STATE (nvarchar)
            - Sales_Zone (nvarchar)
            - REGION_NAME (nvarchar)
            - AREA_NAME (nvarchar)
            - Short_Name (nvarchar)
            - [Sales Offcie Type] (nvarchar) = 'sales Office'
            - Sales Offcie Type (nvarchar)
        # Table 3: DW.fSalesBudget
            - SalesOfficeID (int)
            - MaterialCode (int)
            - FinancialYear (int)
            - PlanMonth (int)
            - BudgetQuantityLPD (decimal)
    
    IMPORTANT NOTE: The columns SalesOfficeID (integer) in the DW.fSales table, PLANT (float) in the DW.dSalesOfficeMaster table, and SalesOfficeID (integer) in the DW.fSalesBudget table all represent the same data. Although their names and data types differ across these tables, they refer to the same entity and can be used to join these tables together.
    > joins_and_mapping_rules: > 
        - join DW.fSales with DW.dSalesOfficeMaster like : select * from dw.fSales f inner join DW.dSalesOfficeMaster d on f.SalesOfficeID = d.PLANT and d.[Sales Offcie Type]='sales Office'
        - join DW.fSalesBudget with DW.dSalesOfficeMaster like: select * from DW.fSalesBudget b inner join DW.dSalesOfficeMaster d on b.SalesOfficeID=d.plant and d.[Sales Offcie Type]='sales Office'
        - SELECT SUM(BudgetQuantityLPD) FROM [worddb].[HFLSQLReader].[Budgets_mapping] b JOIN Dw.dSalesOfficeMaster d ON b.SalesOfficeID = d.PLANT
    
    > date_logic_and_fiscal_rules: >
        - ytd or year to date logic:Financial Year Start: April 1 : WHERE BillingDate >= CASE WHEN MONTH(GETDATE()) >= 4 THEN CAST(YEAR(GETDATE()) AS VARCHAR) + '-04-01' ELSE CAST(YEAR(GETDATE()) - 1 AS VARCHAR) + '-04-01' END AND BillingDate <= CAST(GETDATE() - 1 AS DATE)
        - qtd or quarter to date logic: WHERE BillingDate >= CASE WHEN MONTH(GETDATE()) BETWEEN 4 AND 6 THEN CAST(YEAR(GETDATE()) AS VARCHAR) + '-04-01' WHEN MONTH(GETDATE()) BETWEEN 7 AND 9 THEN CAST(YEAR(GETDATE()) AS VARCHAR) + '-07-01' WHEN MONTH(GETDATE()) BETWEEN 10 AND 12 THEN CAST(YEAR(GETDATE()) AS VARCHAR) + '-10-01' ELSE CAST(YEAR(GETDATE()) AS VARCHAR) + '-01-01' END AND BillingDate <= CAST(GETDATE() - 1 AS DATE) 
        - mtd or month to date logic: Start of current month to yesterday, eg:  WHERE BillingDate >= DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1) AND BillingDate <= CAST(GETDATE() - 1 AS DATE)
        - last week or previous week logic: BETWEEN DATEADD(DAY, -7, DATEADD(WEEK, DATEDIFF(WEEK, 0, GETDATE()), 0)) AND DATEADD(DAY, -1, DATEADD(WEEK, DATEDIFF(WEEK, 0, GETDATE()), 0))
        - lastmonth:  EOMONTH(DATEADD(MONTH, -1, GETDATE()))
        - Completed Days in Current Month: DAY(GETDATE()) - 1
        - opm or over previous month logic: WHERE BillingDate >= DATEFROMPARTS(YEAR(DATEADD(MONTH, -1, GETDATE())), MONTH(DATEADD(MONTH, -1, GETDATE())), 1) AND BillingDate <= EOMONTH(DATEADD(MONTH, -1, GETDATE()))
        - opy or over previous year logic: WHERE BillingDate >= DATEFROMPARTS(YEAR(DATEADD(YEAR, -1, GETDATE())), MONTH(GETDATE()), 1) AND BillingDate <= EOMONTH(DATEFROMPARTS(YEAR(DATEADD(YEAR, -1, GETDATE())), MONTH(GETDATE()), 1))
    
    >keywords: achievement= budget 

    > INSTRUCTIONS:
        learn concept through Main logic examples and generate ssms sql query by understanding user query properly.
        - To get PlanMonth, take the year and month from the target date and combine them as YYYYMM (e.g., for April 2025, year = 2025 and month = 04, so PlanMonth = 202504).
        - NEVER : Don't consider BillingDate for the target date, always use PlanMonth.
    
    > MAIN LOGIC:    
    examples: 
    eg 1) what is the achievement of april 2025 ? SQL: WITH Actual AS (SELECT SUM(SalesQuantity) * 1.0 / 30.0 AS Actual FROM DW.fSales f JOIN Dw.dSalesOfficeMaster d ON f.SalesOfficeID = d.PLANT WHERE BillingDate BETWEEN '2025-04-01' AND '2025-04-30' AND d.[Sales Offcie Type] = 'Sales Office' AND ProductHeirachy1 NOT IN ('Feed', 'Feed Supplement', 'Veterinary Medicines', 'Water', 'HERITA LIV', 'NULL', 'others')), Target AS (SELECT SUM(BudgetQuantityLPD) AS Target FROM [worddb].[HFLSQLReader].[Budgets_mapping] b JOIN Dw.dSalesOfficeMaster d ON b.SalesOfficeID = d.PLANT WHERE PlanMonth = 202504) SELECT a.Actual, t.Target, CASE WHEN t.Target = 0 THEN NULL ELSE (a.Actual / t.Target) * 100 END AS AchievementPercent FROM Actual a, Target t;
    
    yesterday target means : you can consider planmonth like this : b.PlanMonth = CAST(FORMAT(GETDATE() - 1, 'yyyyMM') AS INT)
    lastmonth target means : you can consider planmonth like this : b.PlanMonth = CAST(FORMAT(DATEADD(MONTH, -1, GETDATE()), 'yyyyMM') AS INT)
    like example : 
    eg 2) what is the achievement of lastmonth ? SQL : WITH Actual AS (SELECT SUM(f.SalesQuantity) * 1.0 / DAY(EOMONTH(DATEADD(MONTH, -1, GETDATE()))) AS Actual FROM DW.fSales f JOIN Dw.dSalesOfficeMaster d ON f.SalesOfficeID = d.PLANT WHERE f.BillingDate BETWEEN DATEFROMPARTS(YEAR(DATEADD(MONTH, -1, GETDATE())), MONTH(DATEADD(MONTH, -1, GETDATE())), 1) AND EOMONTH(DATEADD(MONTH, -1, GETDATE())) AND d.[Sales Offcie Type] = 'Sales Office' AND f.ProductHeirachy1 NOT IN ('Feed', 'Feed Supplement', 'Veterinary Medicines', 'Water', 'HERITA LIV', 'NULL', 'others')), Target AS (SELECT SUM(BudgetQuantityLPD) AS Target FROM [worddb].[HFLSQLReader].[Budgets_mapping] b JOIN Dw.dSalesOfficeMaster d ON b.SalesOfficeID = d.PLANT WHERE PlanMonth = CAST(FORMAT(DATEADD(MONTH, -1, GETDATE()), 'yyyyMM') AS INT)) SELECT a.Actual, t.Target, CASE WHEN t.Target = 0 THEN NULL ELSE (a.Actual / t.Target) * 100 END AS AchievementPercent FROM Actual a, Target t;

    eg 3) what is the budget of milk in the hso1 in may 2025 ? SQL : WITH Actual AS (SELECT SUM(f.SalesQuantity) * 1.0 / 31.0 AS Actual FROM DW.fSales f JOIN DW.dSalesOfficeMaster d ON f.SalesOfficeID = d.PLANT WHERE f.BillingDate BETWEEN '2025-05-01' AND '2025-05-31' AND d.[Sales Offcie Type] = 'Sales Office' AND d.Short_Name = 'HSO 1' AND f.ProductHeirachy1 = 'Milk' AND f.ProductHeirachy1 NOT IN ('Feed', 'Feed Supplement', 'Veterinary Medicines', 'Water', 'HERITA LIV', 'NULL', 'others')), Target AS (SELECT SUM(b.BudgetQuantityLPD) AS Target FROM [worddb].[HFLSQLReader].[Budgets_mapping] b JOIN DW.dSalesOfficeMaster d ON b.SalesOfficeID = d.PLANT WHERE b.PlanMonth = 202505 AND d.Short_Name = 'HSO 1' AND d.[Sales Offcie Type] = 'Sales Office' AND ProductHeirachy1 = 'Milk') SELECT a.Actual, t.Target, CASE WHEN t.Target = 0 THEN NULL ELSE (a.Actual / t.Target) * 100 END AS AchievementPercent FROM Actual a, Target t;

    eg 4) what is the budget of milk  in may 2025 ? SQL: WITH Actual AS (SELECT SUM(f.SalesQuantity) * 1.0 / 31.0 AS Actual FROM DW.fSales f JOIN DW.dSalesOfficeMaster d ON f.SalesOfficeID = d.PLANT WHERE f.BillingDate BETWEEN '2025-05-01' AND '2025-05-31' AND d.[Sales Offcie Type] = 'Sales Office' AND f.ProductHeirachy1 = 'Milk' AND f.ProductHeirachy1 NOT IN ('Feed', 'Feed Supplement', 'Veterinary Medicines', 'Water', 'HERITA LIV', 'NULL', 'others')), Target AS (SELECT SUM(b.BudgetQuantityLPD) AS Target FROM [worddb].[HFLSQLReader].[Budgets_mapping] b JOIN DW.dSalesOfficeMaster d ON b.SalesOfficeID = d.PLANT WHERE b.PlanMonth = 202505 AND d.[Sales Offcie Type] = 'Sales Office' AND b.ProductHeirachy1 = 'Milk') SELECT a.Actual, t.Target, CASE WHEN t.Target = 0 THEN NULL ELSE (a.Actual / t.Target) * 100 END AS AchievementPercent FROM Actual a, Target t;

    IMP : The user will give only values (e.g., 'Milk', 'HSO 1'); you must identify the correct column they belong to (like ProductHeirachy1 or Short_Name) and apply the same filter in both Actual and Target queries—do not use unrelated columns.
            
    > edge case terms to handle:
        • If user asks for milk consider the ProductHeirachy1 not Materialgroup
        • If the user query mentions "icecream/fd", "ice cream/fd", "frozen dessert", or any variation that explicitly includes "fd":
             → Then use: Materialgroup = 'ICE CREAM/FD'
        (Make sure it's in uppercase and enclosed in single quotes.)
        • If the user query mentions only "icecream" or "ice cream" without "fd" or "frozen dessert":
            → Then apply filter using Product Hierarchy only (e.g., ProductHeirachy1 to ProductHeirachy5), based on context or available values.
        • Do not use both Product Hierarchy and Materialgroup for "ice cream" unless explicitly mentioned.
        • If the user query mentions "flav.milk' or 'flavoured milk' or 'flav milk' then Consider: ProductHeirachy1 = 'flav.milk'
         'flav.milk' and 'milk' are different but both present in the ProductHeirachy1 search carefully
        IMP: example: user query: what is the sale of toned milk yesterday: SELECT SUM(SalesQuantity)/1 AS MilkSalesQuantity_Yesterday FROM DW.fSales WHERE ProductHeirachy1 = 'Milk' AND BillingDate = CAST(GETDATE() - 1 AS DATE) AND ProductHeirachy1 NOT IN ('Feed', 'Feed Supplement', 'Veterinary Medicines', 'Water', 'HERITA LIV', 'NULL', 'others')
        -- Toned milk means you has to consider milk and milk tm
    

    "I'll tip you $200 if you learn the concept from the examples above and generate an accurate SQL query that satisfies the user input"

> User Query:{user_input}

  SQL:
    Give direct ssms executable query without any explanations
""")

prompt_template_budget2  = PromptTemplate(
    input_variables=["user_input"], 
    template=""" 
Role: You are an expert SQL generator and business analyst for a BI chatbot.

Goal: Convert natural language into accurate, efficient T-SQL using schema and business logic.
- Generalize from examples — extract logic, don’t memorize.
- Use reasoning to infer missing or implicit details.
- Follow business rules, fiscal calendar, join paths, and naming conventions.
- Output queries that are complete, correct, and production-ready.
    
 > tables_and_schema: > Use the following tables and their core columns:
        # Table 1: DW.fSales
            - BillingDate (date)
            - SalesOfficeID (int)
            - CustomerGroup (nvarchar)
            - CustomerID (nvarchar)
            - ProductHeirachy1 (nvarchar)
            - ProductHeirachy2 (nvarchar)
            - ProductHeirachy3 (nvarchar)
            - ProductHeirachy4 (nvarchar)
            - ProductHeirachy5 (nvarchar)
            - ProductHeirachy5 (nvarchar)
            - Materialgroup (nvarchar)
            - SubMaterialgroup1 (nvarchar)
            - SubMaterialgroup2 (nvarchar)
            - SubMaterialgroup3 (nvarchar)
            - MaterialCode (int)
            - SalesQuantity (decimal)
            - nvarchar (decimal)
            - TotalTax (decimal)
            - NetAmount (decimal)
        # Table 2: DW.dSalesOfficeMaster
            - PLANT (float)
            - PLANT_NAME (nvarchar)
            - CITY (nvarchar)
            - STATE (nvarchar)
            - Sales_Zone (nvarchar)
            - REGION_NAME (nvarchar)
            - AREA_NAME (nvarchar)
            - Short_Name (nvarchar)
            - [Sales Offcie Type] (nvarchar) = 'sales Office'
            - Sales Offcie Type (nvarchar)
        # Table 3: DW.fSalesBudget
            - SalesOfficeID (int)
            - MaterialCode (int)
            - FinancialYear (int)
            - PlanMonth (int)
            - BudgetQuantityLPD (decimal)
    
    IMPORTANT NOTE: The columns SalesOfficeID (integer) in the DW.fSales table, PLANT (float) in the DW.dSalesOfficeMaster table, and SalesOfficeID (integer) in the DW.fSalesBudget table all represent the same data. Although their names and data types differ across these tables, they refer to the same entity and can be used to join these tables together.
    > joins_and_mapping_rules: > 
        - join DW.fSales with DW.dSalesOfficeMaster like : select * from dw.fSales f inner join DW.dSalesOfficeMaster d on f.SalesOfficeID = d.PLANT and d.[Sales Offcie Type]='sales Office'
        - join DW.fSalesBudget with DW.dSalesOfficeMaster like: select * from DW.fSalesBudget b inner join DW.dSalesOfficeMaster d on b.SalesOfficeID=d.plant and d.[Sales Offcie Type]='sales Office'
        - SELECT SUM(BudgetQuantityLPD) FROM [worddb].[HFLSQLReader].[Budgets_mapping] b JOIN Dw.dSalesOfficeMaster d ON b.SalesOfficeID = d.PLANT
    
    > date_logic_and_fiscal_rules: >
        - ytd or year to date logic:Financial Year Start: April 1 : WHERE BillingDate >= CASE WHEN MONTH(GETDATE()) >= 4 THEN CAST(YEAR(GETDATE()) AS VARCHAR) + '-04-01' ELSE CAST(YEAR(GETDATE()) - 1 AS VARCHAR) + '-04-01' END AND BillingDate <= CAST(GETDATE() - 1 AS DATE)
        - qtd or quarter to date logic: WHERE BillingDate >= CASE WHEN MONTH(GETDATE()) BETWEEN 4 AND 6 THEN CAST(YEAR(GETDATE()) AS VARCHAR) + '-04-01' WHEN MONTH(GETDATE()) BETWEEN 7 AND 9 THEN CAST(YEAR(GETDATE()) AS VARCHAR) + '-07-01' WHEN MONTH(GETDATE()) BETWEEN 10 AND 12 THEN CAST(YEAR(GETDATE()) AS VARCHAR) + '-10-01' ELSE CAST(YEAR(GETDATE()) AS VARCHAR) + '-01-01' END AND BillingDate <= CAST(GETDATE() - 1 AS DATE) 
        - mtd or month to date logic: Start of current month to yesterday, eg:  WHERE BillingDate >= DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1) AND BillingDate <= CAST(GETDATE() - 1 AS DATE)
        - last week or previous week logic: BETWEEN DATEADD(DAY, -7, DATEADD(WEEK, DATEDIFF(WEEK, 0, GETDATE()), 0)) AND DATEADD(DAY, -1, DATEADD(WEEK, DATEDIFF(WEEK, 0, GETDATE()), 0))
        - lastmonth:  EOMONTH(DATEADD(MONTH, -1, GETDATE()))
        - Completed Days in Current Month: DAY(GETDATE()) - 1
        - opm or over previous month logic: WHERE BillingDate >= DATEFROMPARTS(YEAR(DATEADD(MONTH, -1, GETDATE())), MONTH(DATEADD(MONTH, -1, GETDATE())), 1) AND BillingDate <= EOMONTH(DATEADD(MONTH, -1, GETDATE()))
        - opy or over previous year logic: WHERE BillingDate >= DATEFROMPARTS(YEAR(DATEADD(YEAR, -1, GETDATE())), MONTH(GETDATE()), 1) AND BillingDate <= EOMONTH(DATEFROMPARTS(YEAR(DATEADD(YEAR, -1, GETDATE())), MONTH(GETDATE()), 1))
    
    > INSTRUCTIONS:
        learn concept through Main logic examples and generate ssms sql query by understanding user query properly.
        - To get PlanMonth, take the year and month from the target date and combine them as YYYYMM (e.g., for April 2025, year = 2025 and month = 04, so PlanMonth = 202504).
        - NEVER : Don't consider BillingDate for the target date, always use PlanMonth.
    
    > MAIN LOGIC:

    examples: 
    eg 1) what is top performing salesoffices of april 2025 based on achievement ? SQL: WITH ActualSales AS (SELECT d.Short_Name, SUM(f.SalesQuantity) * 1.0 / 30.0 AS Actual FROM DW.fSales f INNER JOIN DW.dSalesOfficeMaster d ON d.PLANT = f.SalesOfficeID WHERE d.[Sales Offcie Type] = 'sales Office' AND f.BillingDate BETWEEN '2025-04-01' AND '2025-04-30' GROUP BY d.Short_Name), Budget AS (SELECT d.Short_Name, SUM(b.BudgetQuantityLPD) AS Target FROM DW.fSalesBudget b INNER JOIN DW.dSalesOfficeMaster d ON d.PLANT = b.SalesOfficeID WHERE d.[Sales Offcie Type] = 'sales Office' AND b.PlanMonth = 202504 GROUP BY d.Short_Name) SELECT b.Short_Name, b.Target, a.Actual, CASE WHEN b.Target = 0 THEN NULL ELSE (a.Actual / b.Target) * 100 END AS AchievementPercent FROM Budget b LEFT JOIN ActualSales a ON b.Short_Name = a.Short_Name ORDER BY AchievementPercent DESC;
    eg 2) what is top performing salesoffices of lastmonth based on achievement ? SQL: WITH ActualSales AS (SELECT d.Short_Name, SUM(f.SalesQuantity) * 1.0 / DAY(EOMONTH(DATEADD(MONTH, -1, GETDATE()))) AS Actual FROM DW.fSales f INNER JOIN DW.dSalesOfficeMaster d ON d.PLANT = f.SalesOfficeID WHERE d.[Sales Offcie Type] = 'sales Office' AND f.BillingDate BETWEEN DATEFROMPARTS(YEAR(DATEADD(MONTH, -1, GETDATE())), MONTH(DATEADD(MONTH, -1, GETDATE())), 1) AND EOMONTH(DATEADD(MONTH, -1, GETDATE())) GROUP BY d.Short_Name), Budget AS (SELECT d.Short_Name, SUM(b.BudgetQuantityLPD) AS Target FROM DW.fSalesBudget b INNER JOIN DW.dSalesOfficeMaster d ON d.PLANT = b.SalesOfficeID WHERE d.[Sales Offcie Type] = 'sales Office' AND b.PlanMonth = CAST(FORMAT(DATEADD(MONTH, -1, GETDATE()), 'yyyyMM') AS INT) GROUP BY d.Short_Name) SELECT b.Short_Name, b.Target, a.Actual, CASE WHEN b.Target = 0 THEN NULL ELSE (a.Actual / b.Target) * 100 END AS AchievementPercent FROM Budget b LEFT JOIN ActualSales a ON b.Short_Name = a.Short_Name ORDER BY AchievementPercent DESC;

    least or bottom or less means : you can use ORDER BY AchievementPercent ASC
    yesterday target means : you can consider planmonth like this : b.PlanMonth = CAST(FORMAT(GETDATE() - 1, 'yyyyMM') AS INT)
    lastmonth target means : you can consider planmonth like this : b.PlanMonth = CAST(FORMAT(DATEADD(MONTH, -1, GETDATE()), 'yyyyMM') AS INT)


    > edge case terms to handle:
        • If user asks for milk consider the ProductHeirachy1 not Materialgroup
        • If the user query mentions "icecream/fd", "ice cream/fd", "frozen dessert", or any variation that explicitly includes "fd":
             → Then use: Materialgroup = 'ICE CREAM/FD'
        (Make sure it's in uppercase and enclosed in single quotes.)
        • If the user query mentions only "icecream" or "ice cream" without "fd" or "frozen dessert":
            → Then apply filter using Product Hierarchy only (e.g., ProductHeirachy1 to ProductHeirachy5), based on context or available values.
        • Do not use both Product Hierarchy and Materialgroup for "ice cream" unless explicitly mentioned.
        • If the user query mentions "flav.milk' or 'flavoured milk' or 'flav milk' then Consider: ProductHeirachy1 = 'flav.milk'
         'flav.milk' and 'milk' are different but both present in the ProductHeirachy1 search carefully
        IMP: example: user query: what is the sale of toned milk yesterday: SELECT SUM(SalesQuantity)/1 AS MilkSalesQuantity_Yesterday FROM DW.fSales WHERE ProductHeirachy1 = 'Milk' AND BillingDate = CAST(GETDATE() - 1 AS DATE) AND ProductHeirachy1 NOT IN ('Feed', 'Feed Supplement', 'Veterinary Medicines', 'Water', 'HERITA LIV', 'NULL', 'others')
        -- Toned milk means you has to consider milk and milk tm
    

    "I'll tip you $200 if you learn the concept from the examples above and generate an accurate SQL query that satisfies the user input"

> User Query:{user_input}

  SQL:
    Give direct ssms executable query without any explanations
""")


llm = ChatOpenAI(
    temperature=0,
    #model_name="gpt-4-turbo",
    model_name="gpt-3.5-turbo-0125",
    openai_api_key=None 

)

def detect_intent(user_query: str) -> str:
    query = user_query.lower()
    performance_terms = ["performing","perform","perfroming","growing","performing","top perfroming","less performing","less growing","top growing","opm","opy","over previous month","over previous year"]
    budget_terms=["budget", "achievement"]
    
    nrv_terms = [
    "nrv", "nr", "net realization", "net realisation", "net realizaton", "net realisaton","vap", "valud added products", "value added products", "value added product",  "net revenue", "net reveue", "net sales value", "net sales valie", "net proceeds", "net procedeeds","realized value", "realised value", "net amount", "net amont","net income", "net icome", "realization value", "realisation value"]
    
    season_terms = [
    "seasonality", "trend", "BEST MONTS", "best months", "peak mounths", "peak months", "peak sales mounths", "peak sales months", "seasonal patern", "seasonal pattern", "seasonal trennd", "seasonal trend", "sales peak", "demand pattern", "high season","worst season", "worst months", "not performing seasons", "best season","seasonal fluctuation", "sales slump", "off season", "off-seasn", "low demand period", "demand surge", "seasonal variation", "seasonal demand", "performance trend", "sales cycle","sales slmp", "off-seaon", "seasonal variaton", "performance trennd","season",
]   
     
    if any(p in query for p in performance_terms) and any(b in query for b in nrv_terms):
        return "perform_nrv"
    if any(term in query for term in [
    "achievement", "acheivement", "acheivment", "achievment", "achivement", "achevment", "achievemnt", "acheevement", "achivment", "achivemant", "achivemint", "achvmnt", "achvmntt", "achvment","budget", "buget", "budjet", "budgt", "badget", "bidget", "bodget", "bugdet", "budegt", "budgtet", "bidgeet", "budgit", "bugettt", "budet", "budgte"]):
        return "achievement"
    elif any(p in query for p in performance_terms) and any(b in query for b in season_terms):
        return "seasonality"
    elif any(term in query for term in ["sku","stock keeping unit","stop keeping unit","sku's","skus"]):
        return "sku"
    elif any(term in query for term in [
    "unbilled", "unbiled", "unbil", "unbilld", "unblled", "unblid", "unbllled", "unbld","not billed", "not bil", "notbilld", "notbiled", "notblled", "notbllled", "not biled", "notbilled"]):
        return "unbilled"
    elif any(term in query for term in ["billed","ubc","unique billing count","customers",'biled',"unique biling count","uqe blg cout","billing count","bill cnt","billing cnt"]):
        return "billed"
    elif any(term in query for term in [
    "mom", "month on month","growth", "groth", "grouth", "grwoth", "grwth", "grwot", "grwotth", "grothh", "grotht", "grwothh", "grwht", "groeth", "growt", "grouwt", "growht", "gr0wth", "groowth", "grwuth","change","variance", "varience","variance percentage", "variance percantage"]):
        return "mom"
    elif any(term in query for term in [
    "actual", "actaul", "actul", "acutal", "actaul", "actal", "actaul", "acual", "actaul", "actaul","volume", "volum", "volumn", "vollume", "vloume", "volmue", "volome","total volume", "total vol", "total volum", "totl volume", "totl vol", "tatal volume", "toatl volume"]):
        return "actual"
    elif any(term in query for term in [
    "target", "traget", "targt", "trget", "targrt", "targeet", "targat", "tarjet", "targit", "targert","target volume", "target vol", "traget volume", "traget vol", "targt volume", "targt vol", "targit volume", "targit vol", "targeet volume", "targeet vol", "tarjet volume", "tarjet vol"]):
        return "target"
    elif any(term in query for term in [
    "achievement", "acheivement", "acheivment", "achievment", "achivement", "achevment", "achievemnt", "acheevement", "achivment", "achivemant", "achivemint", "achvmnt", "achvmntt", "achvment","budget", "buget", "budjet", "budgt", "badget", "bidget", "bodget", "bugdet", "budegt", "budgtet", "bidgeet", "budgit", "bugettt", "budet", "budgte","actual and target","actual vs target"]):
        return "achievement"
    elif any(term in query for term in ["unique billed customers","ubc", "billed", "customers", "unique billing", "unique billing count", "dne", "unbilled","notbilled", "unique billed customers", "customers"]):
        return "ubc"
    elif any(term in query for term in ["seasonality","season","trend", "BEST MONTS", "peak mounths", "peak sales mounths", "seasonal patern", "seasonal trennd", "sales peak", "demand pattern", "high season","worst season","worst months","not performing seasons"]):
        return "seasonality"
    elif any(term in query for term in ["sale", "sale quantity", "volume", "vol","lpd","sold","quantity", "sale", "sael", "sales", "sles", "sale quantity", "sales quantity", "volume", "vol", "voulme", "slae","units sold", "unit solds", "revenue", "revnue", "turnover", "turnvoer","shipment", "shipmnt", "dispatch", "disptach", "sales volume", "total sales", "sold quantity", "sales amount", "sale amont"]):
        return "sale"
    elif any(term in query for term in ["nr", "loss making","porfit making", "loss","profit","nrv","nrv", "nr", "net realization", "net realisation", "net realizaton", "net realisaton","vap", "valud added products", "value added products", "value added product", "net revenue", "net reveue", "net sales value", "net sales valie", "net proceeds", "net procedeeds", "realized value", "realised value", "net amount", "net amont","net income", "net icome", "realization value", "realisation value"]):
        return "perform_nrv"
    elif any(p in query for p in performance_terms) and any(b in query for b in budget_terms):
        return "budget2"
    else:
        return "all"
    
prompt_map = {
    "budget2":prompt_template_budget2,
    "sale": prompt_template_sale,
    "ubc": prompt_template_ubc,
    "mom": prompt_template_mom,
    "seasonality": prompt_template_seasonality,
    "all":prompt_template_all,
    "perform_nrv":prompt_template_perform_nrv,
    "sku": prompt_template_sku,
    "unbilled": prompt_template_unbilled,
    "billed": prompt_template_billed,
    "actual": prompt_template_actual,
    "target": prompt_template_target,
    "achievement": prompt_template_achievement,
}

def select_prompt(user_query: str):
    intent = detect_intent(user_query)
    return prompt_map[intent]

##prompt_template = select_prompt(user_query)
##nl_to_sql_chain = LLMChain(llm=llm, prompt=prompt_template)

product_hierarchy_terms = {
    'Milk', 'Butter', 'ButterMilk', 'Cheese', 'Cold Coffee', 'Cream', 'Curd', 'Doodh Peda',
    'Flav.Milk', 'Frozen Dessert', 'Ghee', 'Gluco Shakti', 'Gulab Jamun', 'IceCream', 'Laddu', 
    'Lassi', 'Milk Cake', 'Milk Shakes', 'Paneer', 'Rasgulla', 'Shrikhand', 'SkimMilk Powder',
    'Buffalo', 'Cow', 'Default', 'Mixed',
    'Afghan Delight', 'Agmarked', 'Almond Crunch', 'American Delight', 'Amrakhand', 'Anjeer Badam',
    'Badam', 'Badam Nuts', 'Badam Pista Kesar', 'Banana Cinnamon', 'Banana Strawberry', 'Belgium Chocolate',
    'Berry Burst', 'BFCM', 'Black Currant Vanilla', 'Black Current', 'Blocks', 'Bubble Gum', 'Butter Scotch',
    'Butterscotch Bliss', 'Butterscotch Crunch', 'Caramel Nuts', 'Caramel Ripple Sundae', 'Cassatta',
    'Choco chips', 'Choco Rock', 'Chocobar', 'Chocolate', 'Chocolate Coffee Fudge', 'Chocolate Overload',
    'Classic Kulfi', 'Classic Vanilla', 'Coffee', 'Cookies & Cream', 'Cotton Candy', 'Cubes', 'Double Chocolate',
    'DTM', 'Elachi', 'FCM', 'Fig Honey', 'Fruit Fantasy', 'Fruit Fusion', 'Gol Gappa', 'Golden Cow Milk',
    'Grape Juicy', 'Gulkhand Kulfi', 'HONEY NUTS', 'ISI', 'Jowar', 'Kala Khatta', 'Kohinoor Kulfi', 'Laddoo Prasadam',
    'LATTE', 'Low Fat', 'Malai Kulfi', 'Mango', 'Mango Alphanso', 'Mango Juicy', 'Mango Masti Jusy',
    'Mango Tango', 'Mawa Kulfi', 'Mega-Sundae', 'Melon Rush', 'Mixed Berry Sundae', 'Mixed Millet', 'Mozarella',
    'NonAgmarked', 'Orange', 'Orange Juicy', 'Pan Kulfi', 'Pine Apple', 'Pineapple', 'Pista', 'Pistachio',
    'Plain', 'Pot Kulfi (Pista)', 'Premium Vannila', 'Probiotic', 'Probiotic TM', 'Rajbhog', 'Rasperry Twin',
    'Roasted Cashew', 'Royal Rose Delight', 'Sabja', 'Salted', 'Shrikhand Kesar', 'Sitaphal', 'Slices', 'Slim',
    'Special', 'STANDY', 'STD', 'STD Milk', 'Strawberry', 'Strawbery', 'Sweet', 'TM', 'Twin Vanilla&Strawberry',
    'Vanilla', 'Vanilla&Strawberry', 'Alu. Foil Pack', 'Aluminium Foil  Pack', 'Ball', 'Box', 'Bucket', 'Carton',
    'Ceka Pack', 'Cone', 'Cup', 'Glass Bottle', 'Jar', 'Matka', 'Pillow Pack', 'Poly Pack', 'Pouch', 'PP + Box',
    'PP Bottle', 'Sachets', 'Spout Pouch', 'STANDY POUCH', 'Stick', 'Stick (Ice Cream)', 'Tetra Pack', 'Tin', 'Tray',
    'Tub', 'UHT Poly Pack', '1 KG', '10 KG', '100 GMS', '100 ML', '1000 GMS', '1000 ML', '110 ML', '110ML',
    '115 GMS', '120 GMS', '120 ML', '125 GMS', '125 ML', '125ML', '12ML', '130 GMS', '130 ML', '135 GMS', '135 ML',
    '140 GMS', '140 ML', '145 GMS', '145 ML', '15 KG', '150 GMS', '150 ML', '155 ML', '160 GMS', '160 ML', '165 GMS',
    '165 ML', '170 GMS', '170 ML', '175 ML', '18.2 KG', '180 GMS', '180 ML', '185 ML', '190 ML', '2 KG', '2 Litres',
    '20 GMS', '20 KG', '200 GMS', '200 ML', '220 GMS', '220 ML', '225 GMS', '225 ML', '230 ML', '250 GMS', '250 ML',
    '25ML', '300 GMS', '310 ML', '325 ML', '330 ML', '35 ML', '350 GMS', '350 ML', '360 GMS', '375 ML', '380 GMS',
    '4 liters', '4 Litres', '4.5 KG', '4.70 KG', '40 ML', '400 GMS', '400 ML', '425 GMS', '425 ML', '440 ML', '450 GMS',
    '450 ML', '475 GMS', '475 ML', '480 GMS', '480 ML', '485 ML', '490 ML', '5 Kg', '5 Litres', '50 ML', '500 GMS',
    '500 ML', '6 Liter', '60 ML', '60ML', '65 ML', '70 GMS', '70 ML', '700 ML', '700+700ML', '700ML', '750 ML',
    '80 GMS', '80 ML', '800 ML', '850 GMS', '9 KG', '9.1 KG', '90 ML', '900 GMS', '900 ML', '950 GMS', '950 ML',
    '975 ML', '990 ML'
}

def preprocess_user_input(user_input: str) -> str:
    # Replace business terms with SQL expressions
    user_input = replace_business_terms(user_input)
    # Sort terms by length descending to avoid partial replacements
    sorted_terms = sorted(product_hierarchy_terms, key=len, reverse=True)
    for term in sorted_terms:
        # Use regex to replace whole word matches case-insensitively
        pattern = re.compile(r'\b' + re.escape(term) + r'\b', re.IGNORECASE)
        user_input = pattern.sub(f"'{term}'", user_input)
    return user_input

def fix_unquoted_product_terms(sql_query: str) -> str:
    """
    Post-process the generated SQL query to ensure product hierarchy terms are quoted.
    """
    for term in product_hierarchy_terms:
        # Replace unquoted term with quoted term, only if it appears as a standalone word
        pattern = re.compile(rf"(?<!')\b{re.escape(term)}\b(?!')", re.IGNORECASE)
        sql_query = pattern.sub(f"'{term}'", sql_query)
    return sql_query
def clean_sql_query(sql_query):
    """
    Clean SQL query by removing common code block prefixes, optional language specifiers, 
    trailing semicolons, and markdown code block formatting.
    """
    sql_query = sql_query.strip()

    # Remove markdown/code block formatting and language hints (like 'SQL:')
    sql_query = re.sub(r"^\s*(SQL:?|```sql|```)?\s*", "", sql_query, flags=re.IGNORECASE)
    sql_query = re.sub(r"```$", "", sql_query).strip()

    # Remove trailing semicolon if present
    sql_query = sql_query.rstrip(";").strip()

    return sql_query

def generate_sql_from_nl(user_query: str) -> str:
    print(user_query)
    prompt_template = select_prompt(user_query)
    nl_to_sql_chain = LLMChain(llm=llm, prompt=prompt_template)

    print(prompt_template)
   # print("before preprocess: "+ user_query)
    preprocessed_query = preprocess_user_input(user_query)
   # print("Preprocessed Query 1:", preprocessed_query)  # Added print statement for debugging

    # Get all matched entities above threshold
    matched_entities = fuzzy_match_entities(user_query, entities_by_category)

    # Append all matches info to the query prompt
    # preprocessed_query = f"Actual user query: {user_query}\n"
    location_keywords = ['CITY', 'STATE', 'PLANT_NAME', 'Short_Name','REGION_NAME','SHORT_NAME' ]

    preprocessed_query += f".-> Based on strict rules, this is our domain understanding of the user query. ...and the original user query is: {user_query}\n"
    for val, col in matched_entities.items():
        if any(keyword in col.upper() for keyword in location_keywords):
            preprocessed_query += (
    f"'{val}' is mapped to the column '{col}'. "
    "Use this column to filter results as per the user's original query. "
    "Always refer to the correct table aliases: use 'f.' for DW.fSales and 'd.' for DW.dSalesOfficeMaster. "
    "To do this, join DW.fSales with DW.dSalesOfficeMaster using: "
    "SELECT * FROM DW.fSales f "
    "INNER JOIN DW.dSalesOfficeMaster d ON f.SalesOfficeID = d.PLANT "
    "AND d.[Sales Offcie Type] = 'sales Office'.\n"
)  

        else:
            preprocessed_query += (
            f" '{val}' is mapped to the column '{col}'. "
            "Use this column to filter results as per the user's original query.\n"
        )
    print("Main Preprocessed Query:", preprocessed_query) 
    #preprocessed_query=fix_sql_value_quoting

    result = nl_to_sql_chain.run(user_input=preprocessed_query)
    # Remove markdown triple backticks and optional language specifier
    if result.startswith("```sql"):
        result = result[len("```sql"):].strip()
    elif result.startswith("```"):
        result = result[len("```"):].strip()
    # Remove any trailing ```
    if result.endswith("```"):
        result = result[:-3].strip()
    # Fix unquoted product hierarchy terms in SQL

    result = fix_unquoted_product_terms(result)
    result = clean_sql_query(result)
    return result.strip()
