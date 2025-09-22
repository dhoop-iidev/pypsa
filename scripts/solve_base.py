import pypsa
import sys
import os
import re
import pandas as pd

import logging

logger = logging.getLogger(__name__)

from vresutils.benchmark import memory_logger

# Suppress logging of the slack bus choices
pypsa.pf.logger.setLevel(logging.WARNING)

# Add pypsa-earth scripts to path for import
sys.path.insert(0, os.getcwd() + "/pypsa-earth/scripts")

from solve_network import solve_network, prepare_network


def adjust_network(n):
    """
    Adjust network for MGA compatibility, handling pypsa-earth sector coupling
    """

    # 1. Handle sector coupling generators that should be loads
    load_like_carriers = ['load', 'H2 load', 'battery load']
    problematic_gens = n.generators[n.generators.carrier.isin(load_like_carriers)]

    if not problematic_gens.empty:
        logger.warning(f"Found {len(problematic_gens)} generators with load-like carriers")

        # Convert to loads where appropriate
        for idx, gen in problematic_gens.iterrows():
            if gen.carrier == 'load':
                # Convert to regular load
                bus = gen.bus
                if bus not in n.loads.index:
                    n.add("Load",
                          name=f"converted_{idx}",
                          bus=bus,
                          p_set=abs(gen.p_nom))  # Use absolute value
            elif gen.carrier == 'H2 load':
                # Convert to H2 load (if you want to keep H2 coupling)
                # Otherwise just remove
                pass
            elif gen.carrier == 'battery load':
                # These are usually handled by storage units
                pass

        # Remove all problematic generators
        n.generators.drop(problematic_gens.index, inplace=True)
        logger.warning(f"Removed {len(problematic_gens)} problematic generators")

    # 2. Clean up carriers - ensure all used carriers are defined
    if hasattr(n, 'carriers'):
        # Get all carriers actually used in the network
        used_carriers = set()
        if not n.generators.empty:
            used_carriers.update(n.generators.carrier.unique())
        if not n.loads.empty and hasattr(n.loads, 'carrier'):
            used_carriers.update(n.loads.carrier.unique())
        if not n.storage_units.empty:
            used_carriers.update(n.storage_units.carrier.unique())
        if not n.stores.empty:
            used_carriers.update(n.stores.carrier.unique())

        # Remove any None or invalid carriers
        used_carriers.discard(None)
        used_carriers.discard('')

        # Add missing carriers
        missing_carriers = used_carriers - set(n.carriers.index)
        for carrier in missing_carriers:
            logger.info(f"Adding missing carrier: {carrier}")
            n.add("Carrier", carrier)

    # 3. Remove any components with undefined buses
    if not n.generators.empty:
        invalid_bus_gens = n.generators[~n.generators.bus.isin(n.buses.index)]
        if not invalid_bus_gens.empty:
            logger.warning(f"Removing {len(invalid_bus_gens)} generators with invalid buses")
            n.generators.drop(invalid_bus_gens.index, inplace=True)

    # 4. Original MGA adjustments for unique naming
    if not n.lines.empty:
        n.lines.index = ["LN{}".format(i) for i in n.lines.index]
    if not n.links.empty:
        n.links.index = ["LK{}".format(i) for i in n.links.index]

    # 5. Line configuration
    if not n.lines.empty:
        ln_config = snakemake.config["lines"]
        n.lines = n.lines.loc[n.lines.s_nom != 0]
        n.lines.s_max_pu = ln_config["s_max_pu"]
        n.lines.s_nom_min = n.lines.s_nom
        n.lines.s_nom_max = n.lines.apply(
            lambda line: max(
                line.s_nom + ln_config["s_nom_add"],
                line.s_nom * ln_config["s_nom_factor"],
            ),
            axis=1,
        )

    # 6. Link configuration
    if not n.links.empty:
        lk_config = snakemake.config["links"]
        n.links.p_nom_min = n.links.p_nom
        n.links.p_nom_max = float(lk_config["p_nom_max"])

    # 7. Final consistency check
    try:
        n.consistency_check()
        logger.info("Network passed consistency check")
    except Exception as e:
        logger.warning(f"Network consistency issues: {e}")

    return n


if __name__ == "__main__":
    logging.basicConfig(
        filename=snakemake.log.python, level=snakemake.config["logging_level"]
    )

    opts = [
        o
        for o in snakemake.wildcards.opts.split("-")
        if not re.match(r"^\d+h$", o, re.IGNORECASE)
    ]

    with memory_logger(
            filename=getattr(snakemake.log, "memory", None), interval=30.0
    ) as mem:
        n = pypsa.Network(snakemake.input[0])

        logger.info(f"Original network: {len(n.generators)} generators, {len(n.loads)} loads")

        n = adjust_network(n)

        logger.info(f"Adjusted network: {len(n.generators)} generators, {len(n.loads)} loads")

        n = prepare_network(n, solve_opts=snakemake.config["solving"]["options"], config=snakemake.config)
        n = solve_network(
            n,
            config=snakemake.config,
            solving=snakemake.config["solving"],
            log_fn=snakemake.log.solver,
        )

        n.export_to_netcdf(snakemake.output[0])

    logger.info("Maximum memory usage: {}".format(mem.mem_usage))