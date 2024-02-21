import asyncio

from master.component import MasterComponent

from .component import AnalyzerComponent

async def main():
    analyzer_component = AnalyzerComponent()
    master_component = MasterComponent()
    analyzer_event_handler = analyzer_component.disaster_analyzer(
        master_component.consumer_auto_scaler(),
        master_component.producer_observer()
    )
    return await analyzer_event_handler.run()

if __name__ == "__main__":
    asyncio.run(main())
