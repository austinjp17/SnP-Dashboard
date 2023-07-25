
import asyncio
import string_sum
import polar as pl

async def main(): 
    df = pl.DataFrame({
        "foo": [1, 2, None],
        "bar": ["a", None, "c"],
    })
    out_df = my_cool_function(df)
    res = await string_sum.get_data()
    print(res)
    
if __name__ == "__main__":
    asyncio.run(main())