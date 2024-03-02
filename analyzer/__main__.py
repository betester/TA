import asyncio
from fogverse.util import get_config
from master.component import MasterComponent

from .component import AnalyzerComponent

async def main():
    analyzer_component = AnalyzerComponent()
    master_component = MasterComponent()

    mode = str(get_config("ANALYZER_MODE", default="parallel")).strip()

    consumer_auto_scaler = master_component.consumer_auto_scaler()
    producer_observer = master_component.producer_observer()

    if mode == "parallel":
        analyzer_event_handler = analyzer_component.parallel_disaster_analyzer(
            consumer_auto_scaler
        )
        analyzer_event_handler.start(producer_observer.send_total_successful_messages)

    elif mode == "serial":
        await analyzer_component.disaster_analyzer(
            consumer_auto_scaler,
            producer_observer
        ).run()
        

if __name__ == "__main__":
    asyncio.run(main())
