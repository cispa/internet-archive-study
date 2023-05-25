import json
import os
import tldextract

if os.path.exists("trackers.json"):
    all_tracker_sites = set(json.load(open("trackers.json")))
else:
    with open("entities_244c8098aa7313e9bd727aa760911402eeb59676.json") as fh:
        tmp = json.load(fh)
        all_tracker_sites = set()
        for entity_name, entity_info in tmp["entities"].items():
            for domain in entity_info["properties"]:
                all_tracker_sites.add(tldextract.extract(domain).registered_domain)
            for domain in entity_info["resources"]:
                all_tracker_sites.add(tldextract.extract(domain).registered_domain)

    all_tracker_sites -= {''}
    print(all_tracker_sites)
    with open("trackers.json", "w") as fh:
        json.dump(list(all_tracker_sites), fh)