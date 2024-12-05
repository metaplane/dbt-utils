import urllib.request

try:
    # Open the URL
    with urllib.request.urlopen('https://www.google.com') as response:
        # Read the HTML content
        html = response.read()
        
        # Decode the bytes to a string (assuming UTF-8 encoding)
        html_str = html.decode('utf-8')
        
        # Print or save the HTML as needed
        print(html_str)
        

except Exception as e:
    print(f"An error occurred: {e}")