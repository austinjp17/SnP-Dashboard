
import asyncio
import rust
import polars
# from sklearn.metrics import plot_confusion_matrix

async def main(): 
#    pass
    res = await rust.get_arrow_data()
    print(res)
    
if __name__ == "__main__":
    asyncio.run(main())