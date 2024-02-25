import asyncio
from fogverse.util import get_config
from master.component import MasterComponent

from .component import AnalyzerComponent

async def main():
    analyzer_component = AnalyzerComponent()
    master_component = MasterComponent()

    mode = get_config("ANALYZER_MODE", default="serial").strip()

    producer_observer = master_component.producer_observer()
    if mode == "parallel":
        analyzer_event_handler = analyzer_component.parallel_disaster_analyzer()
        analyzer_event_handler.start(producer_observer.send_total_successful_messages)

    elif mode == "serial":
        await analyzer_component.disaster_analyzer(
            master_component.consumer_auto_scaler(),
            master_component.producer_observer()
        ).run()
        

if __name__ == "__main__":
    asyncio.run(main())
