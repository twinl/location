"""
Identifies locations of Twitter users (municipality, province, country) from "location" attribute in tweets.
Focus on fine-grained identification for The Netherlands and difference between tweets from The Netherlands and Belgium.
Run as: gunzip -c data/20210401-??.out.gz | grep '"lang": "nl"' | python3 location/get_locations.py
Output:
Netherlands: 39.5%
Belgium: 4.8%
Netherlands GUESS: 39.7%
Belgium GUESS: 5.4%
"""

import datetime
import json
import pandas as pd
import re
import sys


aliasses = {
    "nederland": "Netherlands",
    "the netherlands": "Netherlands",
    "belgië": "Belgium",
    "antwerpen, belgië": "Belgium",
    "streefkerk": "Molenlanden",
    "hoogvliet": "Rotterdam",
    "pijnacker noord": "Pijnacker-Nootdorp",
    "scheveningen": "Den Haag",
    "dinther": "Bernheze",
    "near rotterdam the netherlands": "Rotterdam",
    "omgeving rotterdam": "Rotterdam",
    "zaandijk": "Zaanstad",
    "westerhoven": "Bergeijk",
    "grunn": "Groningen",
    "district arnhem, reinwich gld.": "Arnhem",
    "hollandscheveld": "Hoogeveen",
    "jordaan": "Amsterdam",
    "dokkum": "Noardeast-Fryslân",
    "mokum": "Amsterdam",
    "driebergen": "Utrechtse Heuvelrug",
    "volendam": "Edam-Volendam",
    "ter apel": "Westerwolde",
    "nederland, steenwijkerland": "Steenwijkerland",
    "nederland, maassluis": "Maassluis",
    "lutjegast": "Westerkwartier",
    "nl - spijkenisse": "Nissewaard",
    "wanneperveen": "Steenwijkerland",
    "holsloot 0613164463": "Coevorden",
    "lemmer": "De Friese Meren",
    "❌❌❌": "Amsterdam",
    "buitengebied brunssumerheide (": "Brunssum",
    "sellingen": "Westerwolde",
    "ochten": "Neder-Betuwe",
    "020": "Amsterdam",
    "amsterdam-noord": "Amsterdam",
    "kaatsheuvel": "Loon op Zand",
    "geboren rotturdammurt": "Rotterdam",
    "khunfang, eindhoven, jomtien": "Rotterdam",
    "rotterdam-ijsselmonde": "Rotterdam",
    "010": "Rotterdam",
    "the hague | austin, tx": "Den Haag",
    "sleen": "Coevorden",
    "de rijp": "Alkmaar",
    "monnickendam": "Waterland",
    "ginneken": "Breda",
    "@🏠, leeuwarden , friesland, 🇪🇺": "Leeuwarden",
    "oude noorden, rotterdam": "Rotterdam",
    "stiens": "Leeuwarden",
    "baarlo (lb)": "Peel en Maas",
    "zwaag": "Hoorn",
    "efteling": "Loon op Zand",
    "menaldum": "Waadhoeke",
    "glanerbrug": "Enschede",
    "040": "Eindhoven",
    "loon": "Assen",
    "mijdrecht": "De Ronde Venen",
    "duivendrecht": "Ouder-Amstel",
    "wezep": "Oldebroek",
    "metropool amsterdam": "Amsterdam",
    "regio alkmaar": "Alkmaar",
    "malden": "Heumen",
    "nieuwolda": "Oldambt",
    "074": "Hengelo",
    "netherlands, groningen": "Groningen",
    "init, amsterdam": "Amsterdam",
    "noordwolde, bedum": "Het Hogeland",
    "siegerswoude": "Opsterland",
    "soestdijk": "Soest",
    "rotterdam-capelle ad ijssel nl": "Rotterdam",
    "berkel en rodenrijs": "Lansingerland",
    "breskens, sluis & terneuzen": "Sluis",
    "lent, nijmegen": "Nijmegen",
    "hofstad": "Den Haag",
    "nijverdal": "Hellendoorn",
    "biddinghuizen": "Dronten",
    "noordwolde": "Weststellingwerf",
    "52.326147,4.855987": "Amsterdam",
    "schoonhoven": "Krimpenerwaard",
    "schoonhoven-oude stad": "Krimpenerwaard",
    "beilen": "Midden-Drenthe",
    "vleuten": "Utrecht",
    "nl-overbetuwe": "Overbetuwe",
    "made": "Drimmelen",
    "nederland, amsterdam": "Amsterdam",
    "roelofarendsveen": "Kaag en Braassem",
    "den-haag": "Den Haag",
    "iphone: 52.221008,6.895315": "Enschede",
    "the haque": "Den Haag",
    "amsterdam.z.o.": "Amsterdam",
    "eys": "Gulpen-Wittem",
    "valkenburg netherland": "Valkenburg aan de Geul",
    "annen, aa en hunze": "Aa en Hunze",
    "amerongen": "Utrechtse Heuvelrug",
    "kralingen-oost": "Rotterdam",
    "072": "Alkmaar",
    "vinkeveen": "De Ronde Venen",
    "overstegen-oost, doetinchem": "Doetinchem",
    "the netherlands rotterdam": "Rotterdam",
    "winschoten": "Oldambt",
    "twello": "Voorst",
    "bunschoten-spakenburg": "Bunschoten",
    "ÜT: 51.629194,5.311012".lower(): "Boxtel",
    "overijssel, deventer": "Deventer",
    "schaarsbergen, arnhem": "Arnhem",
    "marum": "Westerkwartier",
    "alphen aan den rijn. netherlands": "Alphen aan den Rijn",
    "drentse in groningen ☘️🌷🌳": "Groningen",
    "rijnsburg": "Katwijk",
    "anna paulowna": "Hollands Kroon",
    "onderbanken": "Beekdaelen",
    "nuth": "Beekdaelen",
    "oirsbeek, schinnen": "Beekdaelen",
    "wijdenes": "Drechterland",
    "centrum hoogkarspel": "Drechterland",
    "goose meren": "Gooise Meren",
    "aarle-rixtel": "Laarbeek",
    "lieshout": "Laarbeek",
    "hooge mierde": "Reusel-De Mierde",
    "mook": "Mook en Middelaar",
    "berg 22c nuenen T: 040 2831675": "Nuenen, Gerwen en Nederwetten",
    "nuenen-eeneind": "Nuenen, Gerwen en Nederwetten",
    "de tienden 48 5674 tb nuenen": "Nuenen, Gerwen en Nederwetten",
    "nuenen-noord": "Nuenen, Gerwen en Nederwetten",
    "sint odiliënberg": "Roerdalen",
    "gemeente sudwest fryslan": "Sudwest Fryslan",
    "hurdegarijp": "Tietjerksteradeel",
    "hurdegaryp": "Tietjerksteradeel",
    "appingedam": "Eemsdelta",
    "stad appingedam": "Eemsdelta",
    "appingedam (gr)": "Eemsdelta",
    "delfzijl": "Eemsdelta",
    "netherland, delfzijl": "Eemsdelta",
    "loppersum": "Eemsdelta",
    "the hague": "Den Haag",
    "hoofddorp": "Haarlemmermeer",
    "voorburg": "Leidschendam-Voorburg",
    "spijkenisse": "Nissewaard",
    "zaandam": "Zaanstad",
    "bussum": "Gooise Meren",
    "'s-gravenhage": "Den Haag",
    "drachten": "Smallingerland",
    "bodegraven": "Bodegraven-Reeuwijk",
    "leyden": "Leiden",
    "bilthoven": "De Bilt",
    "rosmalen": "'s-Hertogenbosch",
    "ijmuiden": "Velsen",
    "maarssen": "Stichtse Vecht",
    "den bosch": "'s-Hertogenbosch",
    "emmeloord": "Noordoostpolder",
    "leidschendam": "Leidschendam-Voorburg",
    "sneek": "Súdwest-Fryslân",
    "steenwijk": "Steenwijkerland",
    "súdwest fryslân": "Súdwest-Fryslân",
    "naarden": "Gooise Meren",
    "nieuw-lekkerland": "Molenlanden",
    "leerdam": "Vijfheerenlanden",
    "oud-beijerland": "Hoeksche Waard",
    "pijnacker": "Pijnacker-Nootdorp",
    "vianen": "Vijfheerenlanden",
    "voorburg oud, leidschendam-voo": "Leidschendam-Voorburg",
    "hoofddorp of elders": "Haarlemmermeer",
    "bedum": "Het Hogeland",
    "haren": "Groningen",
    "the hague netherlands": "Den Haag",
    "nuenen, gerwen en nederwetten, nederland": "Nuenen, Gerwen en Nederwetten",
    "noord holland": "Noord-Holland",
    "north holland": "Noord-Holland",
    "fryslân": "Friesland",
    "fryslan": "Friesland",
    "veluwe, gelderland, nederland": "Gelderland",
    "ijmond": "Noord-Holland",
    "thuis in west friesland": "Noord-Holland",
    "netherlands, noord-brabant": "Noord-Brabant",
    "north brabant": "Noord-Brabant",
    "south holland": "Zuid-Holland",
    "west gelderland": "Gelderland",
    "zuid holland": "Zuid-Holland",
    "nederland (nb) en spanje (gc)": "Noord-Brabant",
    "eu nederland utrecht n'gein": "Nieuwegein",
    "north-brabant": "Noord-Brabant",
    "haaglanden": "Zuid-Holland",
    "noord brabant": "Noord-Brabant",
    "zeeuws-vlaanderen": "Zeeland",
    "rijnmond. binnenkort buiten eu": "Zuid-Holland",
    "nederland, overijssel": "Overijssel",
    "nederland, den haag": "Den Haag",
    "twente": "Overijssel",
    "achterhoek": "Gelderland",
    "8erhoek": "Gelderland",
    "veluwe , gld": "Gelderland",
    "nederland": "Netherlands",
    "the netherlands": "Netherlands",
    "nl": "Netherlands",
    "holland": "Netherlands",
    "🇳🇱": "Netherlands",
    "🇱🇺": "Luxembourg",
    "belgie": "Belgium",
    "belgië": "Belgium",
    "be": "Belgium",
    "vlaanderen": "Belgium",
    "brussels": "Belgium",
    "brussel": "Belgium",
    "antwerpen": "Belgium",
    "gent": "Belgium",
    "ghent": "Belgium",
    "antwerp": "Belgium",
    "leuven": "Belgium",
    "brugge": "Belgium",
    "kortrijk": "Belgium",
    "mechelen": "Belgium",
    "broekzele": "Belgium",
    "berlin": "Germany",
    "berghem-noord, Oss": "Oss",
    "belgique": "Belgium",
    "bruxelles": "Belgium",
    "polska": "Poland",
    "italia": "Italy",
    "jakarta": "Indonesia",
    "london": "United Kingdom",
    "paris": "France",
    "roma": "Italy",
    "milano": "Italy",
    "cymru": "United Kingdom",
    "jakarta selatan": "Indonesia",
    "İstanbul".lower(): "Turkey",
    "los angeles": "USA",
    "atlanta": "USA",
    "united states": "USA",
    "jakarta capital region": "Indonesia",
    "kota surabaya": "Indonesia",
    "méxico": "Mexico",
    "california": "USA",
    "chicago": "USA",
    "bandung": "Indonesia",
    "houston": "USA",
    "florida": "USA",
    "new york": "USA",
    "washington": "USA",
    "brasil": "Brazil",
    "são paulo": "Brazil",
    "hong kong": "China",
    "buenos aires": "Argentina",
    "rio de janeiro": "Brazil",
    "lima": "Peru",
    "hasselt": "Belgium",
    "españa": "Spain",
    "deutschland": "Germany",
    "rijssen": "Rijssen-Holten",
    "toronto": "Canada",
    "cape town": "South Africa",
    "manchester": "United Kingdom",
    "madrid": "Spain",
    "pretoria": "South Africa",
    "west-vlaanderen": "Belgium",
    "barcelona": "Spain",
    "johannesburg": "South Africa",
    "uk": "United Kingdom",
    "🇧🇪": "Belgium",
    "🇸🇷": "Suriname",
    "oostende": "Belgium",
    "republic of the philippines": "Philippines",
    "midden nederland": "Netherlands",
    "boston": "USA",
    "miami": "USA",
    "oosterbeek": "Renkum",
    "aalst": "Belgium",
    "randstad": "Netherlands",
    "hoofddorp - pax": "Haarlemmermeer",
    "hoogezand": "Midden-Groningen",
    "rusland": "Russia",
    "brazilië": "Brazil",
    "duitsland": "Germany",
    "drentse in groningen 🚲🌳🐈👣": "Netherlands",
    "🆓🇳🇱⌨️✍️": "Netherlands",
    "www.bnmo.nl": "Netherlands",
    "sint-niklaas": "Belgium",
    "griekenland": "Greece",
    "vernederland": "Netherlands",
    "#vernederland": "Netherlands",
    "regio eindhoven": "Eindhoven",
    "knokke": "Belgium",
    "knokke (be)": "Belgium",
    "noord-nederland.": "Netherlands",
    "48.635324,-1.510687": "France",
    "079": "Zoetermeer",
    "ibiza": "Spain",
    "wolfheze": "Renkum",
    "boechout": "Belgium",
    "vlaming in hart en nieren": "Belgium",
    "schelluinen": "Molenlanden",
    "nevada": "USA",
    "munsterbilzen": "Belgium",
    "netherlands - gouda": "Gouda",
    "vriezenveen the netherlands": "Twenterand",
    "stockholm": "Sweden",
    "the netherlands den haag": "Den Haag",
    "zwitserland": "Switzerland",
    "frankrijk": "France",
    "somewhere in the netherlands": "Netherlands",
    "nedersaksen": "Germany",
    "walking in memphis": "USA",
    "westaan": "Zaanstad",
    "rotterdam-delft": "Rotterdam",
    "côte d'azur":"France",
    "veluwe": "Gelderland",
    "genemuiden": "Zwartewaterland",
    "kingdom of the netherlands": "Netherlands",
    "n-brabant": "Noord-Brabant",
    "fioringras zwolle": "Zwolle",
    "Dutchland": "Netherlands",
    "🇳🇱 & 🇩🇪": "Neherlands",
    "var": "France",
    "netherland": "Netherlands",
    "madurodam": "Den Haag",
    "bolsward": "Súdwest-Fryslân",
    "nederlands neanderthalië": "Netherlands",
    "Я бы хотел похоронить в России".lower(): "Russia",
    "wuustwezel": "Belgium",
    "burgum": "Tietjerksteradeel",
    "nb 52.307326 | ol 5.04261": "Weesp",
    "ÜT: 52.217724,4.865828".lower(): "De Ronde Venen",
}

def get_user_data_from_line(line):
    json_line = json.loads(line)
    user_id = json_line['user']['id_str']
    location = json_line['user']['location']
    date = datetime.datetime.strptime(json_line['created_at'], "%a %b %d %H:%M:%S %z %Y")
    date_string = date.strftime("%Y%m%d:%H:%M:%S")
    user_data = {'user_id': user_id,
                 'location': location,
                 'start_date': date_string,
                 'end_date': date_string,
                 'count': 1
                 }
    return user_data


def get_mentions_from_line(line):
    json_line = json.loads(line)
    return [ mention['id_str'] for mention in json_line['entities']['user_mentions'] ]


def update_user_location_date(user_data_list, user_data):
    for data in user_data_list:
        if data['location'] == user_data['location'] or (data['location'] == None and user_data['location'] == None):
            if user_data['start_date'] < data['start_date']:
                data['start_date'] = user_data['start_date']
            if user_data['end_date'] > data['end_date']:
                data['end_date'] = user_data['end_date']
            data['count'] += 1
            return True
    return False


def get_location_data_from_stdin():
    location_data = {}
    mention_data = {}
    for line in sys.stdin:
        user_data = get_user_data_from_line(line)
        user_id = user_data['user_id']
        if user_id not in location_data:
            location_data[user_id] = [user_data]
        else:
            location_found = update_user_location_date(location_data[user_id], user_data)
            if not location_found:
                location_data[user_id].append(user_data)
        for mention_user_id in get_mentions_from_line(line):
            if user_id not in mention_data:
                mention_data[user_id] = set()
            mention_data[user_id].add(mention_user_id)
            if mention_user_id not in mention_data:
                mention_data[mention_user_id] = set()
            mention_data[mention_user_id].add(user_id)
    return location_data, mention_data


def get_municipalities():
    municipalities_df = pd.read_csv("../../puregome/notebooks/csv/municipalities-wikipedia.csv")
    municipalities_dict = {}
    for i in range(0, len(municipalities_df)):
        municipality = municipalities_df.iloc[i]['municipality'].strip().lower()
        province = municipalities_df.iloc[i]['province'].strip().lower()
        country = "Nederland".strip().lower()
        data = { 'municipality': municipalities_df.iloc[i]['municipality'],
                 'province': municipalities_df.iloc[i]['province'],
                 'country': "Netherlands" }
        if municipality not in municipalities_dict:
            municipalities_dict[municipality] = []
        municipalities_dict[municipality].append(data)
        municipality_province = f"{municipality}, {province}"
        if municipality_province not in municipalities_dict:
            municipalities_dict[municipality_province] = []
        municipalities_dict[municipality_province].append(data)
        municipality_country = f"{municipality}, {country}"
        if municipality_country not in municipalities_dict:
            municipalities_dict[municipality_country] = []
        municipalities_dict[municipality_country].append(data)
    return municipalities_dict


def get_provinces():
    province_names = ("Drenthe Flevoland Friesland Gelderland Groningen Limburg Noord-Brabant " +
                     "Noord-Holland Overijssel Utrecht Zeeland Zuid-Holland").split()
    provinces = {}
    for province in province_names:
        provinces[province.lower()] = [{ "municipality": "", "province": province, "country": "Netherlands" }]
    return provinces


def get_countries():
    country_names = ("Argentina Aruba Belgium Brazil Canada Chile China Colombia Curaçao France Germany Ghana " +
                     "Greece Guatemala India Indonesia Italy Japan Luxembourg Malaysia Mexico Namibia Netherlands " +
                     "Nigeria Norway Peru Philippines Poland Portugal Russia " +
                     "Senegal Singapore Spain Suriname Sweden Switzerland Turkey USA Venezuela").split()
    countries = {}
    for country in country_names:
        countries[country.lower()] = [{ "municipality": "", "province": "", "country": country }]
    countries["south africa"] = [{ "municipality": "", "province": "", "country": "South Africa" }]
    countries["united kingdom"] = [{ "municipality": "", "province": "", "country": "United Kingdom" }]
    return countries


def alias(location):
    if location in aliasses:
        return aliasses[location]
    return(location)


def identify_location(location, municipalities:{}, provinces:{}, countries:{}):
    location = re.sub(r'\s+', ' ', location).strip().lower()
    if location in municipalities:
        return municipalities[location]
    elif location in provinces:
        return provinces[location]
    elif location in countries:
        return countries[location]

    location_alias = alias(location).strip().lower()
    if location_alias in municipalities:
        return municipalities[location_alias]
    elif location_alias in provinces:
        return provinces[location_alias]
    elif location_alias in countries:
        return countries[location_alias]

    location_before_comma = re.sub(r'[,/(].*$', '', location).strip().lower()
    if location_before_comma in municipalities:
        return municipalities[location_before_comma]
    elif location_before_comma in provinces:
        return provinces[location_before_comma]
    elif location_before_comma in countries:
        return countries[location_before_comma]

    location_alias_before_comma = alias(location_before_comma).strip().lower()
    if location_alias_before_comma in municipalities:
        return municipalities[location_alias_before_comma]
    elif location_alias_before_comma in provinces:
        return provinces[location_alias_before_comma]
    elif location_alias_before_comma in countries:
        return countries[location_alias_before_comma]

    location_after_comma = re.sub(r'^.*[,/]', '', location).strip().lower()
    if location_after_comma in municipalities:
        return municipalities[location_after_comma]
    elif location_after_comma in provinces:
        return provinces[location_after_comma]
    elif location_after_comma in countries:
        return countries[location_after_comma]

    location_alias_after_comma = alias(location_after_comma).strip().lower()
    if location_alias_after_comma in municipalities:
        return municipalities[location_alias_after_comma]
    elif location_alias_after_comma in provinces:
        return provinces[location_alias_after_comma]
    elif location_alias_after_comma in countries:
        return countries[location_alias_after_comma]

    location_strip_non_char = re.sub(r'\W+$', '', location)
    if location_strip_non_char != "" and location_strip_non_char != location:
        if location_strip_non_char in municipalities:
            return municipalities[location_strip_non_char]
        elif location_strip_non_char in provinces:
            return provinces[location_strip_non_char]
        elif location_strip_non_char in countries:
            return countries[location_strip_non_char]

    for province in provinces:
        if re.search(province, location, flags=re.IGNORECASE):
            return(provinces[province])
    if re.search(r'(netherland|nederland|holland|pays bas|dutch)', location, flags=re.IGNORECASE):
        return(countries['netherlands'])
    if re.search(r'(belg)', location, flags=re.IGNORECASE):
        return(countries['belgium'])

    return {}


def identify_locations(locations_df, municipalities:{}, provinces:{}, countries:{}):
    results = []
    not_found = [{ 'municipality': "", 'province': "", 'country': "" }]
    for i in range(0, len(locations_df)):
        location = locations_df.iloc[i]['location']
        if type(location) != type(None):
            identify_location_result = identify_location(location, municipalities, provinces, countries)
            if len(identify_location_result) > 0:
                results.append(identify_location_result)
            else:
                results.append(not_found)
        else:
            results.append(not_found)
    return(results)


def empty(data):
    return(data == None or data == "")


def coverage(locations_df):
    locations_found, municipalities_found, provinces_found, countries_found = 0, 0, 0, 0
    total_tweets = 0
    for i, row in locations_df.iterrows():
        if not empty(row['location']):
            locations_found += row['count']
        if not empty(row['municipality']):
            municipalities_found += row['count']
        if not empty(row['province']):
            provinces_found += row['count']
        if not empty(row['country']):
            countries_found += row['count']
        total_tweets += row['count']
    print(f"coverage: locations: {round(100*locations_found/total_tweets, 1)}%", end="; ")
    print(f"municipalities: {round(100*municipalities_found/total_tweets, 1)}%", end="; ")
    print(f"provinces: {round(100*provinces_found/total_tweets, 1)}%", end="; ")
    print(f"countries: {round(100*countries_found/total_tweets, 1)}%")


def missed_locations(location_df):
    missed = {}
    for i, row in locations_df.iterrows():
        if not empty(row['location']) and empty(row['municipality']) and \
           empty(row['province']) and empty(row['country']):
            if not row['location'] in missed:
                missed[row['location']] = 0
            missed[row['location']] += row['count']
    missed = {key: missed[key] for key in sorted(missed, key=lambda key: missed[key], reverse=True)}
    return pd.DataFrame([{'key':key, 'count':missed[key]} for key in missed])


def get_network(mention_data, locations_df):
    network = {}
    for mention_user_id in mention_data:
       if mention_user_id in locations_df.index:
            country = locations_df.loc[mention_user_id]['country']
            if country != "":
                if country not in network:
                    network[country] = 0
                network[country] += 1
    return(network)


def get_most_frequent_keys(data):
    most_frequent_key = []
    most_frequent_value = 0
    for key in data:
        if data[key] > most_frequent_value:
            most_frequent_key = [key]
            most_frequent_value = data[key]
        elif data[key] == most_frequent_value:
            most_frequent_key.append(key)
    return(most_frequent_key)


def guess_locations(location_df, mention_data):
    locations_df.set_index('user_id', inplace=True)
    countries_of_users = { "Netherlands": 0, "Belgium": 0, "Netherlands GUESS": 0, "Belgium GUESS": 0 }
    total_tweets = 0
    for user_id in locations_df.index:
        count = locations_df.loc[user_id]['count']
        total_tweets += count
        if locations_df.loc[user_id]['country'] == "Netherlands":
            countries_of_users["Netherlands"] += count
        elif locations_df.loc[user_id]['country'] == "Belgium":
            countries_of_users["Belgium"] += count
        elif locations_df.loc[user_id]['country'] == "" and user_id in mention_data:
            network = get_network(mention_data[user_id], locations_df)
            most_frequent_keys = get_most_frequent_keys(network)
            if len(most_frequent_keys) == 1:
                if most_frequent_keys[0] == "Netherlands":
                    countries_of_users["Netherlands GUESS"] += count
                if most_frequent_keys[0] == "Belgium":
                    countries_of_users["Belgium GUESS"] += count
    for key in countries_of_users:
        print(f"{key}: {round(100*countries_of_users[key]/total_tweets, 1)}%")


municipalities_dict = get_municipalities()
provinces = get_provinces()
countries = get_countries()

location_data, mention_data = get_location_data_from_stdin()
locations_df = pd.DataFrame([location_data[user][0] for user in location_data])

results = identify_locations(locations_df, municipalities_dict, provinces, countries)
locations_df['municipality'] = [ results[i][0]['municipality'] for i in range(0, len(results)) ]
locations_df['province'] = [ results[i][0]['province'] for i in range(0, len(results)) ]
locations_df['country'] = [ results[i][0]['country'] for i in range(0, len(results)) ]

coverage(locations_df)
print(missed_locations(locations_df)[45:105])

guess_locations(locations_df, mention_data)
