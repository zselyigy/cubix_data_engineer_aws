import json
from icecream import ic

# open the json file we work with
file_path = r'.\data\json_handling\spotify_playlist.json'
with open(file_path, 'r') as json_file:
    data = json.load(json_file)

# display the name of the first track
ic(data['contents']['items'][0]['name'])
# display the play count and name of all tracks
for track in data['contents']['items']:
    ic(track['playCount'], track['name'])