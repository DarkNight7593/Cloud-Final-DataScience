const AWS = require('aws-sdk');
const { Client } = require('@elastic/elasticsearch');

const es = new Client({ node: 'http://34.233.20.17:9201' }); // Cambia IP si es necesario

exports.handler = async (event) => {
  for (const record of event.Records) {
    const eventName = record.eventName;

    try {
      if (eventName === 'INSERT' || eventName === 'MODIFY') {
        const newImage = AWS.DynamoDB.Converter.unmarshall(record.dynamodb.NewImage);

        const esId = `${newImage.tenant_id}#${newImage.curso_id}`;

        const doc = {
          curso_id: newImage.curso_id,
          tenant_id: newImage.tenant_id,
          nombre: newImage.nombre,
          descripcion: newImage.descripcion,
          inicio: newImage.inicio,
          fin: newImage.fin,
          precio: newImage.precio,
          horarios: newImage.horarios || [] // puede venir de otros flujos
        };

        await es.index({
          index: 'cursos',
          id: esId,
          document: doc
        });

      } else if (eventName === 'REMOVE') {
        const oldKeys = AWS.DynamoDB.Converter.unmarshall(record.dynamodb.Keys);
        const esId = `${oldKeys.tenant_id}#${oldKeys.curso_id}`;

        await es.delete({
          index: 'cursos',
          id: esId
        }).catch(err => {
          if (err.meta?.statusCode !== 404) throw err;
          console.warn(`Documento no encontrado para eliminar: ${esId}`);
        });
      }

    } catch (err) {
      console.error(`Error procesando evento ${eventName}:`, err);
    }
  }

  return { statusCode: 200 };
};
