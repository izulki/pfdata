
import axios from 'axios';
import PTCGConfig from './utils/axios_ptcg_config';

const url = 'https://www.premierleague.com/stats/top/players/goals?se=-1&cl=-1&iso=-1&po=-1?se=-1'; // URL we're scraping
const AxiosInstance = axios.create(PTCGConfig()); // Create a new Axios Instance

// Send an async HTTP Get request to the url
AxiosInstance.get("https://api.pokemontcg.io/v2/sets")
  .then( // Once we have data returned ...
    response => {
      const html = response.data; // Get the HTML from the HTTP request
      console.log(html);
    }
  )
  .catch(console.error); // Error handling