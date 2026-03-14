import os
import time
import json
from dotenv import load_dotenv
import google.generativeai as genai

from database import get_unprocessed_proverbs, update_proverbs_ai_data

load_dotenv()

# Configure Gemini
api_key = os.getenv("GEMINI_API_KEY")
if not api_key:
    raise ValueError("GEMINI_API_KEY environment variable is not set.")

genai.configure(api_key=api_key)

MODEL_NAME = "gemini-2.5-flash"

def model_name_to_source(model_name):
    if model_name.startswith("gemini-"):
        model_name = model_name[len("gemini-"):]
    return f"Gemini {model_name.replace('-', ' ').title()}"

MODEL_SOURCE = model_name_to_source(MODEL_NAME)
model = genai.GenerativeModel(MODEL_NAME)

BATCH_SIZE = 50
TARGET_RPM = 15
# Ceiling division keeps the effective request rate at or below TARGET_RPM.
SLEEP_TIME_SECONDS = (60 + TARGET_RPM - 1) // TARGET_RPM

SYSTEM_INSTRUCTION = f"""
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
        "translation_source": "{MODEL_SOURCE}",
        "meaning_source": "{MODEL_SOURCE}",
    "confidence": 0.95,
    "needs_review": 0.0
  }
]
The `confidence` score should be a float between 0.0 (utterly unsure) and 1.0 (perfectly confident).
If you find a proverb confusing or ambiguous, lower the confidence score and set `needs_review` to 1.0.
CRITICAL: Do not hallucinate new IDs. Only return objects for the IDs provided in the input array.
"""

def process_batch(proverbs_batch):
    print(f"Sending batch of {len(proverbs_batch)} proverbs to Gemini...")
    expected_ids = {p['id'] for p in proverbs_batch}
    
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

            if not isinstance(ai_results, list):
                print("Error: Gemini response is not a JSON array. Failing batch.")
                return None

            validated_results = []
            returned_ids = set()
            unknown_ids = []
            duplicate_ids = []

            for result in ai_results:
                if not isinstance(result, dict):
                    unknown_ids.append("<non-object-item>")
                    continue

                result_id = result.get('id')
                if isinstance(result_id, str):
                    try:
                        result_id = int(result_id)
                        result['id'] = result_id
                    except ValueError:
                        pass

                if result_id not in expected_ids:
                    unknown_ids.append(result_id)
                    continue

                if result_id in returned_ids:
                    duplicate_ids.append(result_id)
                    continue

                # Enforce provenance consistency with the configured model.
                result['translation_source'] = MODEL_SOURCE
                result['meaning_source'] = MODEL_SOURCE

                validated_results.append(result)
                returned_ids.add(result_id)

            if unknown_ids:
                preview = unknown_ids[:10]
                print(f"WARNING: Ignoring {len(unknown_ids)} unexpected response item(s) with unknown IDs/items: {preview}")

            if duplicate_ids:
                preview = duplicate_ids[:10]
                print(f"WARNING: Ignoring {len(duplicate_ids)} duplicate response item(s) for IDs: {preview}")

            missing_ids = sorted(expected_ids - returned_ids)
            if missing_ids:
                preview = missing_ids[:10]
                print(f"ERROR: Gemini response is missing {len(missing_ids)} expected ID(s): {preview}. Failing batch to avoid partial/corrupt updates.")
                return None

            if not validated_results:
                print("Error: No valid items remained after response validation. Failing batch.")
                return None

            return validated_results
            
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
            
        # 4. Sleep to respect the TARGET_RPM limit
        print(f"Sleeping for {SLEEP_TIME_SECONDS} seconds to respect the <= {TARGET_RPM} RPM limit...")
        time.sleep(SLEEP_TIME_SECONDS)
        
    print(f"\nFinished processing. Total proverbs augmented this run: {total_processed}")

if __name__ == "__main__":
    main()
