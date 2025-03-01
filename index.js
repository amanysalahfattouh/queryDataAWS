const mongo = require('mongodb');
const MongoClient = mongo.MongoClient;
const AWS = require('aws-sdk');
const { Parser } = require('json2csv');
const s3 = new AWS.S3();
const url = "mongodb://" + process.env.IP + ":" + process.env.PORT + "/";
const dbname = process.env.DB_NAME;
const s3Bucket = process.env.S3_BUCKET_NAME;
let cachedDb = null;
const caseCollection = "case";

function connectToDatabase(uri) {
    if (cachedDb && cachedDb.client.topology && cachedDb.client.topology.isConnected()) {
        console.log('=> using cached database instance');
        return Promise.resolve(cachedDb.db);
    }
    return MongoClient.connect(uri, {
        useUnifiedTopology: true,
        useNewUrlParser: true
    })
        .then(client => {
            cachedDb = { client, db: client.db(dbname) };
            console.log('=> connected to database:', dbname);
            return cachedDb.db;
        })
        .catch(err => {
            console.error('=> error connecting to database:', err);
            throw err;
        });
}

async function getLastSuccessfulDate() {
    try {
        const objects = await s3.listObjectsV2({
            Bucket: s3Bucket,
            Prefix: 'dataQuery/'
        }).promise();

        if (!objects.Contents || objects.Contents.length === 0) {
            console.log('=> no objects found in S3 bucket, initial run');
            return null;
        }
        const sortedObjects = objects.Contents
            .sort((a, b) => b.LastModified - a.LastModified);
        const mostRecentObject = sortedObjects[0];
        const metadata = await s3.headObject({
            Bucket: s3Bucket,
            Key: mostRecentObject.Key
        }).promise();

        const lastSuccessfulDate = metadata.Metadata.lastsuccessfuldate;
        if (!lastSuccessfulDate) {
            console.log('=> no lastSuccessfulDate found in metadata, initial run');
            return null;
        }
        const timestamp = new Date(lastSuccessfulDate).getTime();
        console.log('=> retrieved lastSuccessfulDate timestamp:', timestamp);
        return timestamp;
    } catch (error) {
        console.error('Error retrieving last successful date:', error);
        throw error;
    }
}

async function getStudyDetails(db, lastSuccessfulTimestamp) {
    try {
        const collection = db.collection(caseCollection);
        let query = {
            status: { $in: ["ACCEPTED", "FINISHED"] }
        };
        const endDate = lastSuccessfulTimestamp
            ? new Date(lastSuccessfulTimestamp)
            : new Date();
        const startDate = new Date(endDate);
        startDate.setDate(endDate.getDate() - 21);
        const oneWeekBefore = new Date(endDate);
        oneWeekBefore.setDate(endDate.getDate() - 7);

        const startTimestamp = new mongo.Timestamp({
            t: Math.floor(startDate.getTime() / 1000),
            i: 1
        });
        const endTimestamp = new mongo.Timestamp({
            t: Math.floor(oneWeekBefore.getTime() / 1000),
            i: 1
        });
        query.finishingDate = {
            $gte: startTimestamp,
            $lt: endTimestamp
        };
        console.log('=> MongoDB Timestamp query range:', {
            start: startTimestamp,
            end: endTimestamp
        });
        const results = await collection.find(query).toArray();
        console.log(`=> Found ${results.length} documents in the specified range`);
        const processedResults = results.map(doc => {
            const processedDoc = { ...doc };
            try {
                if (doc.finishingDate) {
                    if (doc.finishingDate instanceof mongo.Timestamp) {
                        processedDoc.finishingDate = doc.finishingDate.high * 1000;
                    } else if (doc.finishingDate instanceof Date) {
                        processedDoc.finishingDate = doc.finishingDate.getTime();
                    } else if (typeof doc.finishingDate === 'number') {
                        processedDoc.finishingDate = doc.finishingDate * 1000;
                    } else {
                        console.warn(`Unexpected finishingDate type for document ${doc._id}:`, typeof doc.finishingDate);
                        processedDoc.finishingDate = Date.now();
                    }
                } else {
                    console.warn(`Document ${doc._id} has no finishingDate`);
                    processedDoc.finishingDate = Date.now();
                }
            } catch (err) {
                console.error(`Error processing finishingDate for document ${doc._id}:`, err);
                processedDoc.finishingDate = Date.now();
            }
            return processedDoc;
        });
        return {
            results: processedResults,
            startDate: startDate.toISOString().split('T')[0], // Format as YYYY-MM-DD
            endDate: oneWeekBefore.toISOString().split('T')[0] // Format as YYYY-MM-DD
        };
    } catch (error) {
        console.error('Error fetching study details from MongoDB:', error);
        throw error;
    }
}

async function saveToS3(data, currentTimestamp, startDate, endDate) {
    try {
        const processedData = data.map(item => ({
            _id: item._id.toString(),
            assignee: item.assignee,
            'dicom_modality': item.dicom?.modality,
            'dicom_patientage': item.dicom?.patientAge,
            contrast: item.contrast,
            emergency: item.emergency,
            requesttype: item.requestType,
            organization_id: item.organization_id,
            tags: item.tags
        }));
        const fields = [
            '_id', 'assignee', 'dicom_modality', 'dicom_patientage',
            'contrast', 'emergency', 'requesttype',
            'organization_id', 'tags'
        ];
        const json2csvParser = new Parser({ fields });
        const csvData = json2csvParser.parse(processedData);
        const filename = `dataQuery/studies_from_${startDate}_to_${endDate}.csv`;
        await s3.putObject({
            Bucket: s3Bucket,
            Key: filename,
            Body: csvData,
            ContentType: 'text/csv',
            Metadata: {
                createdBy: 'MongoDB-Lambda',
                creationDate: new Date().toISOString(),
                lastSuccessfulDate: new Date(currentTimestamp).toISOString()
            },
        }).promise();
        return filename;
    } catch (error) {
        console.error('S3 upload error:', error);
        throw error;
    }
}

exports.handler = async (event, context) => {
    context.callbackWaitsForEmptyEventLoop = false;
    console.log("Received event:", JSON.stringify(event, null, 2));
    try {
        const db = await connectToDatabase(url);
        const lastSuccessfulTimestamp = await getLastSuccessfulDate();
        console.log('=> Last successful timestamp:', lastSuccessfulTimestamp);
        const { results, startDate, endDate } = await getStudyDetails(db, lastSuccessfulTimestamp);
        console.log(`=> Found ${results.length} results`);
        if (results.length === 0) {
            return {
                statusCode: 204,
                body: JSON.stringify({
                    message: `No new data since ${lastSuccessfulTimestamp}`,
                })
            };
        }
        const currentTimestamp = Date.now();
        const filename = await saveToS3(results, currentTimestamp, startDate, endDate);
        return {
            statusCode: 200,
            body: JSON.stringify({
                message: 'Data exported to S3',
                filename: filename,
                recordCount: results.length,
                lastSuccessfulTimestamp: currentTimestamp
            })
        };
    } catch (error) {
        console.error('Handler error:', error);
        return {
            statusCode: 500,
            body: JSON.stringify({
                message: 'Internal server error',
                error: error.message
            })
        };
    }
};