TRANSFER_MARKT_ROOT_URL = "https://www.transfermarkt.com"
REQUEST_HEADERS = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'}

CLUB_PROFILE_CSV_COLUMN = ["Name", "league"]
PLAYER_PROFILE_CSV_COLUMN = ["MarketValue","Name","BackNumber","Club","National","DateOfBirth","PlaceOfBirth","Age","Height","Position","Foot","Joined","ContractExpires","Outfitter"]

DEFAULT_DATA_DICT = {
    'Date of birth': '-', 
    'Place of birth': '-', 
    'Age': '-', 
    'Height': '-', 
    'Position': '-', 
    'Foot': '-', 
    'Joined': '-', 
    'Contract expires': '-', 
    'Outfitter': '-'
}