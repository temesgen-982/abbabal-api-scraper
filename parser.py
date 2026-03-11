import re

def clean_message(text):
    """
    Cleans message text from the ababaloch channel.
    Removes common hashtags, extra whitespace, and specific signatures if they exist.
    """
    if not text:
        return ""
    
    # Example cleaning: remove hashtags like #Proverb #Amharic
    text = re.sub(r'#\w+', '', text)
    
    # Remove excessive newlines/spaces
    text = re.sub(r'\n{3,}', '\n\n', text)
    
    return text.strip()
