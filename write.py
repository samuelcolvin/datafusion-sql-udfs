import csv

with open("data.csv", mode="w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(["a", "b"])
    for i in range(200):
        writer.writerow([i, i + 201])
