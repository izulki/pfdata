require('dotenv').config();

export default function AWSConfig() :object {
    return {
        accessKeyId: process.env.AWS_ACCESS,
        secretAccessKey: process.env.AWS_SECRET
      };
}