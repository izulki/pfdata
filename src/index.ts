import CollectSets from "./collect/collectSets";

async function main() {
    let collectedSets = await CollectSets();
    console.log(collectedSets);
}

main();

