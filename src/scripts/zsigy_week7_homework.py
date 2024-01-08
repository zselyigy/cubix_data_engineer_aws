# Week 7 Homework
# Created by István Gy. Zsély
# version 0.1
# For details look in the zsigy_week7_homework_README.md file.

import json

# Task 1 Download the playlist.json from the shared folder, open it with Python.
file_path = r'.\data\json_handling\spotify_playlist.json'
with open(file_path, 'r') as json_file:
    data = json.load(json_file)

# task 2 Print the name of the 31st track. Hint: iterate through the file using
# enumerate() built in python function.
print(data['contents']['items'][30]['name'])    # Why to use enumerate()?

# task 3 In these 50 tracks, what is the total playcount? Print the number itself only.
# task 4 Which track has the lowest playcount? Print it out in this format
# "The lowest playcount is {lowest_playcount}." Hint: Use "f string".
total_playcount = 0
lowest_playcount = 1e100                       # this is a really big number, bigger than any expected playCount

for track in data['contents']['items']:
    total_playcount += track['playCount']      # collect the actual playcount
    if track['playCount'] < lowest_playcount:  # store the actual playcount if it is smaller than any before
        lowest_playcount = track['playCount']

print(total_playcount)                         # print the total playcount
print(f'The lowest playcount is {lowest_playcount}.')    # print the lowest playcount
