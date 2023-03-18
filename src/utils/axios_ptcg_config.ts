require('dotenv').config();

export default function PTCGConfig() :object {
    return {
        method: 'get',
        maxBodyLength: Infinity,
        headers: { 
          'X-Api-Key': process.env.PTCG_API
        }
      };
}