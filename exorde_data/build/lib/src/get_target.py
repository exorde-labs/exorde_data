"""
`get_target` is used across all services to retrive their different output
address.

# It parameters,

    `filter_value`: str => the label associated with the target service

        .possible used values are:
            - `transactioneer`
            - `upipe`
            - `bpipe`

        .possible unsused,
            - `orchestrator`

    `preloaded_target_env_key`: str => env variable key for user customization

        .convention
            - "`SERVICE_TYPE_CAPITAL_NAME`_TARGET"

        .possible used values are
            - `TRANSACTIONEER_ADDR`
            - `UPIPE_ADDR`
            - `BPIPE_ADDR`

        .possible unused
            - `ORCHESTRATOR_ADDR`


# How it works,

- It reaches `orchestrator` using the docker-compose network interface, infering 
`ORCHESTRATOR_NAME` from it's env, or staticly "orchestrator" in a fail-over.

- The `/get` endpoint from the `orchestrator` is reponsible for returning a list
of ips

- The function then randomly select one of the ips (ruban).

"""

import logging, os, random
from aiohttp import ClientSession

def log_environment():
    logging.info("Environment Variables:")
    for key, value in os.environ.items():
        logging.info(f"{key}: {value}")

async def get_target(
    filter_value: str,
    preloaded_target_env_key: str,
    filter_key:str = "network.exorde.service"
):
    """Asks the orchestrator for a list of services matching parameters"""
    async def fetch_ips_from_orchestrator(
        filter_key: str, filter_value:str
    ) -> list[str]:
        orchestrator_name = os.getenv("ORCHESTRATOR_NAME", "orchestrator")
        logging.info(f"fetch_ips_from_orchestrator, orchestrator_name is '{orchestrator_name}'")
        assert orchestrator_name
        base_url = f"http://{orchestrator_name}:8000/get"
        query_params = {filter_key: filter_value}
        logging.info(f"\t query_params are {filter_key}: {filter_value}")
        log_environment()
        try:
            async with ClientSession() as session:
                async with session.get(base_url, params=query_params) as response:
                    if response.status == 200:
                        ips = await response.json()
                        logging.info(f"fetch_ips_from_orchestrator - {ips}")
                        return ips
                    else:
                        error_message = await response.text()
                        logging.info(f"Failed to fetch IPs: {error_message}")
                        return []
        except:
            logging.exception("Failed to fetch IPS")
            return []
    pre_loaded_targets = os.getenv(preloaded_target_env_key, '')
    logging.info(f"get_targe.pre_loaded_targets = {pre_loaded_targets}")
    if len(pre_loaded_targets) == 0:
        targets = await fetch_ips_from_orchestrator(filter_key, filter_value)
    else:
        if ',' in pre_loaded_targets:
            targets = pre_loaded_targets.split(',')
        else:
            targets = pre_loaded_targets
    logging.info(f"get_target.targets = {targets}")
    if len(targets) > 0:
        choice = random.choice(targets)
        logging.info(f"get_target.choice = {choice}")
        return choice
    return None
