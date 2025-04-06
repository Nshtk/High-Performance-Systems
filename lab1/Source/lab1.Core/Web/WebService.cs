using System;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace lab1.Core.Web;

public class WebService
{
	private readonly HttpClient _client;

	public WebService()
	{
		Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
		_client = new HttpClient(new HttpClientHandler() { AutomaticDecompression = DecompressionMethods.All });
	}

	public async Task<string> getAsync(string uri)
	{
		using HttpResponseMessage response = await _client.GetAsync(uri);

		return await response.Content.ReadAsStringAsync();
	}

	public async Task<string> postAsync(string uri, string data, string contentType)
	{
		using HttpContent content = new StringContent(data, Encoding.UTF8, contentType);

		HttpRequestMessage requestMessage = new HttpRequestMessage()
		{
			Content = content,
			Method = HttpMethod.Post,
			RequestUri = new Uri(uri)
		};

		using HttpResponseMessage response = await _client.SendAsync(requestMessage);

		return await response.Content.ReadAsStringAsync();
	}
}