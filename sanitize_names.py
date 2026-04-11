import os
import re
import time
import requests
from pymongo import MongoClient
from dotenv import load_dotenv
from rapidfuzz import fuzz

# Ensure igdb_service.py is in the same folder
from igdb_service import IGDBService 

load_dotenv()

# --- CONFIGURATION ---
client = MongoClient(os.getenv("MONGO_URI"))
db = client['gamesDB']
collection = db['fitgirl-games-duplicate']

igdb = IGDBService()
RAWG_KEY = os.getenv("RAWG_API_KEY")

# --- OPTIONAL: RESET ALL GAMES ---
# Run this once if you want to re-process games that were skipped/marked True before.
collection.update_many({}, {"$set": {"sanitized": False}})

# --- CLEANING ENGINE ---
def strip_editions(name):
    """
    Removes common edition suffixes, junk tags, and repack info.
    """
    noise_patterns = [
        r" -? ?\b(Digital )?Deluxe( Edition)?\b",
        r" -? ?\b(Super )?Ultimate( Edition)?\b",
        r" -? ?\bGold( Edition)?\b",
        r" -? ?\bGame of the Year( Edition)?\b",
        r" -? ?\bGOTY\b",
        r" -? ?\bComplete( Edition)?\b",
        r" -? ?\bEnhanced( Edition)?\b",
        r" -? ?\bDefinitive( Edition)?\b",
        r" -? ?\bLegendary( Edition)?\b",
        r" -? ?\bSpecial( Edition)?\b",
        r" -? ?\bCollector'?s( Edition)?\b",
        r" -? ?\bRemastered\b",
        r" -? ?\bFitGirl( Repack)?\b",
        r" -? ?\bWindows 7 Fix\b",
        r" -? ?\bDay 1 Patch\b",
        r" -? ?\bOnline Co-op\b",
        r" \+ \d+ DLCs?.*",
        r" \+ .*Fix.*",
        r" -? ?v?\d+\.\d+.*", 
        r" -? ?\bStandard( Edition)?\b",
    ]
    
    clean_name = name
    for pattern in noise_patterns:
        clean_name = re.sub(pattern, "", clean_name, flags=re.IGNORECASE)
    
    # Clean up trailing punctuation left behind
    clean_name = re.sub(r"[:\- / \+\(\)]+$", "", clean_name)
    return clean_name.strip()

# --- API FETCHERS ---
def get_clean_name_rawg(search_query):
    url = f"https://api.rawg.io/api/games?key={RAWG_KEY}&search={search_query}&page_size=1"
    try:
        res = requests.get(url).json()
        if res.get('results'):
            return res['results'][0]['name']
    except:
        return None
    return None

def get_clean_name_igdb(search_query):
    # This uses your existing igdb_service.py
    data = igdb.fetch_game_metadata(search_query)
    return data.get('name') if data else None

# --- MAIN RUNNER ---
def sanitize_database():
    # Find games where sanitized is False or doesn't exist
    query = {"sanitized": {"$ne": True}}
    all_games = list(collection.find(query))
    
    if not all_games:
        print("✅ No unsanitized games found. Reset 'sanitized' to False if you want to rerun.")
        return

    print(f"🚀 Starting Deep Clean for {len(all_games)} games...")

    for game in all_games:
        original = game.get('gameName', '')
        if not original: continue

        # STEP 1: Pre-clean to help API search
        search_term = strip_editions(original)

        # STEP 2: Fetch from APIs
        rawg_official = get_clean_name_rawg(search_term)
        igdb_official = get_clean_name_igdb(search_term)

        # STEP 3: Advanced Scoring
        rawg_score = fuzz.WRatio(original, rawg_official) if rawg_official else 0
        igdb_score = fuzz.WRatio(original, igdb_official) if igdb_official else 0
        
        best_candidate = None
        current_max_score = max(rawg_score, igdb_score)

        if rawg_score > igdb_score and rawg_score > 65:
            best_candidate = rawg_official
        elif igdb_score >= rawg_score and igdb_score > 65:
            best_candidate = igdb_official
        
        # STEP 4: Apply Update
        if best_candidate:
            final_name = strip_editions(best_candidate)
            collection.update_one(
                {"_id": game["_id"]},
                {"$set": {
                    "gameName": final_name,
                    "original_full_title": best_candidate,
                    "original_messy_name": original,
                    "sanitized": True,
                    "confidence_score": current_max_score
                }}
            )
            print(f"✨ API MATCH: [{original[:25]}...] -> [{final_name}]")
        else:
            # FALLBACK: Clean the original string manually if API fails
            fallback_name = strip_editions(original)
            collection.update_one(
                {"_id": game["_id"]},
                {"$set": {
                    "gameName": fallback_name,
                    "original_messy_name": original,
                    "sanitized": True,
                    "note": "Manual Fallback"
                }}
            )
            print(f"⚠️ FALLBACK: [{original[:25]}...] -> [{fallback_name}]")

        time.sleep(0.4) 

if __name__ == "__main__":
    sanitize_database()
    print("✅ Process Complete.")