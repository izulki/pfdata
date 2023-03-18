"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
require('dotenv').config();
function PTCGConfig() {
    return {
        method: 'get',
        maxBodyLength: Infinity,
        headers: {
            'X-Api-Key': process.env.PTCG_API
        }
    };
}
exports.default = PTCGConfig;
//# sourceMappingURL=axios_ptcg_config.js.map