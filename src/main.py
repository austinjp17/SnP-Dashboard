
import asyncio
import string_sum
import polars
# from sklearn.metrics import plot_confusion_matrix

async def main(): 
#    pass
    res = await string_sum.get_data()
    print("Res: ", res, type(res))
    
if __name__ == "__main__":
    asyncio.run(main())