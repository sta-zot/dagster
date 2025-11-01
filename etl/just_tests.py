from pandas import DataFrame

df = DataFrame(
    {
        "v_id": [1, 2, 3],
        "name": [4, 5, 6],
        "type": [7, 8, 9]
    }
)
df2 = DataFrame(
    {
        "f_id": [2, 3, 4],
        "name": [5, 6, 7],
        "type": [8, 9, 10]
    }
)
df3 = df.merge(df2, on=["name", "type"], indicator=True, how="right")

df3["keys"] = 'laskd;askl'
print(df3)

