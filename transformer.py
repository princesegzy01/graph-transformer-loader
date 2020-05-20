import pandas as pd


def VertexLocation(filename):
    print("VertextLocation")

    df = pd.read_csv(filename, sep = ',')
    print(df.head())

VertexLocation("Data/Location/alabama/alabama.csv")