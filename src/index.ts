import CollectCards from "./collect/collectCards";
import CollectSets from "./collect/collectSets";

async function main() {
    //Date
    const timeElapsed = Date.now();
    const today = new Date(timeElapsed);

    //Meta, //Prices, //Images 
    /** SET COLLECTION */
    let SetMetaFlag = true;
    let SetImagesFlag = false;

    //Every Saturday (Day 6), Collect Images
    if (today.getDay() == 6) SetImagesFlag = true;
        console.log(`${today.toUTCString()}`," - ", ` STARTING SET COLLECTION: (${SetMetaFlag} ${SetImagesFlag})`)
    let collectedSets = await CollectSets(SetMetaFlag, SetImagesFlag);
        console.log(`${today.toUTCString()}`," - ", ` COMPLETED SET COLLECTION: Result: ${collectedSets}`)


    //let collectedCards = await CollectCards();
   //console.log(`${today.toUTCString()}`," - ", " collectedCards status: ", collectedCards)

    //console.log(collectedCards);
}

main();

