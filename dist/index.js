"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const axios_1 = __importDefault(require("axios"));
const axios_ptcg_config_1 = __importDefault(require("./utils/axios_ptcg_config"));
const url = 'https://www.premierleague.com/stats/top/players/goals?se=-1&cl=-1&iso=-1&po=-1?se=-1'; // URL we're scraping
const AxiosInstance = axios_1.default.create((0, axios_ptcg_config_1.default)()); // Create a new Axios Instance
// Send an async HTTP Get request to the url
AxiosInstance.get("https://api.pokemontcg.io/v2/sets")
    .then(// Once we have data returned ...
// Once we have data returned ...
response => {
    const html = response.data; // Get the HTML from the HTTP request
    console.log(html);
})
    .catch(console.error); // Error handling
//# sourceMappingURL=index.js.map