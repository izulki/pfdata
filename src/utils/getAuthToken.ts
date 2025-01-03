require('dotenv').config();

interface TokenResponse {
    access_token: string;
    token_type: string;
    expires_in: number;
  }
  
  export async function getAuthToken(): Promise<string> {
    const tokenUrl = 'https://api.tcgplayer.com/token';
    const formData = new URLSearchParams({
      grant_type: 'client_credentials',
      client_id: process.env.TCGP_PUBLIC,
      client_secret: process.env.TCGP_PRIVATE
    });
  
    const response = await fetch(tokenUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded'
      },
      body: formData
    });
  
    if (!response.ok) {
      throw new Error(`Failed to get auth token: ${response.statusText}`);
    }
  
    const data: TokenResponse = await response.json();
    return data.access_token;
  }