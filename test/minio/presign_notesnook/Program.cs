using Amazon;
using Amazon.S3;
using Amazon.S3.Model;

if (args.Length != 8)
{
    Console.Error.WriteLine("usage: endpoint access-key secret-key bucket get-key put-key multipart-key upload-id");
    return 2;
}

var config = new AmazonS3Config
{
    ServiceURL = args[0],
    AuthenticationRegion = "us-east-1",
    ForcePathStyle = true,
    UseHttp = true,
};
AWSConfigsS3.UseSignatureVersion4 = true;

using var client = new AmazonS3Client(args[1], args[2], config);
var expires = DateTime.UtcNow.AddMinutes(5);

async Task<string> Sign(string key, HttpVerb verb, string? uploadId = null)
{
    var request = new GetPreSignedUrlRequest
    {
        BucketName = args[3],
        Key = key,
        Verb = verb,
        Protocol = Protocol.HTTP,
        Expires = expires,
        UploadId = uploadId,
    };
    if (uploadId != null)
    {
        request.PartNumber = 1;
    }
    return await client.GetPreSignedURLAsync(request);
}

var result = new
{
    Get = await Sign(args[4], HttpVerb.GET),
    Put = await Sign(args[5], HttpVerb.PUT),
    UploadPart = await Sign(args[6], HttpVerb.PUT, args[7]),
};
Console.WriteLine("NOTESNOOK_URLS:" + System.Text.Json.JsonSerializer.Serialize(result));
return 0;
