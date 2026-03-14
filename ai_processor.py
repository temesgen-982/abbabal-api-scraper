import os
import time
import json
import asyncio
from dotenv import load_dotenv
import google.generativeai as genai

from database import get_unprocessed_proverbs, update_proverbs_ai_data

load_dotenv()

# Configure Gemini
api_key = os.getenv("GEMINI_API_KEY")
if not api_key:
    raise ValueError("GEMINI_API_KEY environment variable is not set.")

genai.configure(api_key=api_key)

# We use gemini-1.5-flash to bypass the aggressive free-tier rate limits of 2.5-pro.
model = genai.GenerativeModel("gemini-2.5-flash") 

BATCH_SIZE = 50
SLEEP_TIME_SECONDS = 4 # Wait 4s to stay under 15 RPM limit

SYSTEM_INSTRUCTION = """
You are an expert translator specializing in Amharic proverbs and idioms. 
You will be provided with a JSON array containing pairs of IDs and raw Amharic proverbs.
Your task is to analyze each proverb and return a strictly formatted JSON array containing the translation and meanings. 

For each ID provided, you must return an object with the following structure:
[
  {
    "id": <the original number id provided>,
    "english_translation": "The direct, accurate translation into English",
    "amharic_meaning": "The deeper meaning or context of the proverb explained in Amharic",
    "english_meaning": "The deeper meaning or context of the proverb explained in English",
    "translation_source": "Gemini 1.5 Flash",
    "meaning_source": "Gemini 1.5 Flash",
    "confidence": 0.95,
    "needs_review": 0.0
  }
]
The `confidence` score should be a float between 0.0 (utterly unsure) and 1.0 (perfectly confident).
If you find a proverb confusing or ambiguous, lower the confidence score and set `needs_review` to 1.0.
CRITICAL: Do not hallucinative new IDs. Only return objects for the IDs provided in the input array.
"""

def process_batch(proverbs_batch):
    print(f"Sending batch of {len(proverbs_batch)} proverbs to Gemini...")
    
    # Prepare the payload (just IDs and the text to save tokens)
    payload = [{"id": p['id'], "text": p['text']} for p in proverbs_batch]
    
    prompt = f"{SYSTEM_INSTRUCTION}\n\nHere is the input array:\n{json.dumps(payload, ensure_ascii=False)}"
    
    try_count = 0
    while True:
        try:
            # We enforce a strict JSON application response format
            response = model.generate_content(
                prompt,
                generation_config=genai.GenerationConfig(
                    response_mime_type="application/json"
                )
            )
            
            # Parse the JSON string Gemini returns
            ai_results = json.loads(response.text)
            
            # Validate that Gemini didn't drop or hallucinate items
            if len(ai_results) != len(proverbs_batch):
                print(f"WARNING: Sent {len(proverbs_batch)} items but received {len(ai_results)} back. Continuing anyway with matched items.")
                
            return ai_results
            
        except Exception as e:
            error_msg = str(e)
            if "429" in error_msg or "Quota exceeded" in error_msg:
                print(f"\n[RATE LIMIT HIT] Google API requested a pause. Sleeping for 60 seconds before retrying this batch...")
                time.sleep(60)
                try_count += 1
                if try_count > 5:
                    print("Failed after 5 rate limit retries. Aborting.")
                    return None
            else:
                print(f"Error calling Gemini API: {e}")
                return None

def main():
    print("Starting AI augmentation processor...")
    
    total_processed = 0
    
    while True:
        # 1. Fetch the next batch of un-translated rows
        batch = get_unprocessed_proverbs(limit=BATCH_SIZE)
        
        if not batch:
            print("No more unprocessed proverbs found. Done!")
            break
            
        print(f"\nFetched {len(batch)} unprocessed rows. (Total processed so far: {total_processed})")
        
        # 2. Send to Gemini
        ai_results = process_batch(batch)
        
        if ai_results:
            # 3. Save directly to DB
            try:
                update_proverbs_ai_data(ai_results)
                total_processed += len(ai_results)
                print(f"Successfully wrote {len(ai_results)} parsed results to the database.")
            except Exception as e:
                print(f"Failed to write batch to database: {e}")
                # We break here to avoid endless loops failing on the same corrupt data
                break 
        else:
            print("Failed to get a valid response from Gemini for this batch. Stopping.")
            break
            
        # 4. Sleep to respect the 5 RPM rate limit
        print(f"Sleeping for {SLEEP_TIME_SECONDS} seconds to respect API rate limits...")
        time.sleep(SLEEP_TIME_SECONDS)
        
    print(f"\nFinished processing. Total proverbs augmented this run: {total_processed}")

if __name__ == "__main__":
    main()
