# Week 7 Homework
Created by István Gy. Zsély, version 0.1

# Project overview
This code was created during my participation in the [CUBIX Data Engineering course](https://courses.cubixedu.com/kepzes/data-engineer-23q4).

The aim is to demonstrate the knowledge about the basic handling of JSON.

The following tasks are performed by the program, denoted in comments in the source code:

1. Download the playlist.json from the shared folder, open it with Python.

2. Print the name of the 31st track. Hint: iterate through the file using enumerate() built in python function.

3. In these 50 tracks, what is the total playcount? Print the number itself only.

4. Which track has the lowest playcount? Print it out in this format "The lowest playcount is {lowest_playcount}." Hint: Use "f string".

# Questions:
Task 2: Why use enumerate()? The json structure can be easily accessed by indexing.

Task 4: The question is about identifying the least popular track, but the printout does not ask for the number or name of the track, but for the lowest playcount. This seems a bit inconsistent to me.