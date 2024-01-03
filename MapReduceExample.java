package actividad3;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
public class MapReduceExample {
// Fase de Map
	public static List<Map.Entry<String, Integer>> mapFunction(String document) {
	    // Divide el documento en palabras y filtra solo las palabras [WARNING], [INFO] o [SEVERE],
	    // luego cuenta la frecuencia de cada palabra.
	    List<String> words = Arrays.asList(document.toLowerCase().split("\\s+"));
	    List<String> filteredWords = words.stream()
	            .filter(word -> word.equals("[warn]") || word.equals("[info]") || word.equals("[severe]"))
	            .collect(Collectors.toList());
	    return filteredWords.stream()
	            .collect(Collectors.toMap(word -> word, word -> 1, Integer::sum))
	            .entrySet().stream()
	            .collect(Collectors.toList());
	}
// Fase de Reduce
public static Map.Entry<String, Integer> reduceFunction(Map.Entry<String,
List<Integer>> entry) {
// Reduce: Suma las frecuencias de una palabra.
String word = entry.getKey();
int sum = entry.getValue().stream().mapToInt(Integer::intValue).sum();
return Map.entry(word, sum);
}
public static void main(String[] args) {
// Conjunto de documentos
List<String> documents = Arrays.asList(
		"﻿wallet-rest-api|INFO|16-02-22 19:04:50| [INFO] + whatChanget passTypeId: 'pass.com.qaroni.abanca.presco', deviceLibrary Id: '17884c73fb285457bceefcac86138214', updatedDate: 'null' ]",
		"wallet-rest-api|SEVERE|16-02-22 19:04:52| [SEVERE] + getUpdateCard: [ passTypeId: 'pass.com.qaroni.abanca.presco', serial: '3d36ffb0-16ba-4dac-8f89-7788fa69355b' ]",
		"wallet-rest-api|INFO|16-02-22 19:04:52| [INFO] + getUpdateCard: [ passTypeId: 'pass.com.qaroni.abanca.presco', serial: '6a5992a0-b829-4818-a528-12f87a661490' ]",
		"wallet-rest-api|WARN|16-02-22 19:05:14| [WARN] + editCardPATCH: [customerUUID='a2df404f-3eab-4f12-97b3-4d664d399435', account UUID: 'a07853b7-225e-46eb-9db2-0c4565ea25ca', templateUUID: '6237de75-439f-4704-9f0e-4029' ]",
		"wallet-rest-api|WARN|16-02-22 19:05:17| [WARN] + registerDevice: [ deviceLibraryId='96577bb29e78f1240ecabc1f929da6f9', passTypeId: 'pass.com.qaroni.abanca.presco', serial: 'c00a20de-59a9-4671-b291-1d07779fccdd' ]",
		"wallet-rest-api|WARN|16-02-22 19:05:17| [WARN] + registerDevice: [ deviceLibraryId='a256dfbd0b2309b0570ab811d76b2274', passTypeId: 'pass.com.qaroni.abanca.xunta', serial: '557751c4-6ae0-48d2-afca-d8ed4ea622a6' ]",
		"wallet-rest-api|INFO|16-02-22 19:05:17| [INFO] + registerDevice: [deviceLibraryId='96577bb29e78f1240ecabc1f929da6f9', passTypeId: 'pass.com.qaroni.abanca.presco', serial: 'c2c764db-9fcb-45d7-af75-14da7e535be4' ]",
		"wallet-rest-api|INFO|16-02-22 19:05:17| [INFO] + registerDevice: [ deviceLibraryId='96577bb29e78f1240ecabc1f929da6f9', passTypeId: 'pass.com.qaroni.abanca.presco', serial: '36164b2d-60bb-47c7-b5ed-5dd08848b408' ]",
		"wallet-rest-api|SEVERE|16-02-22 19:05:17| [SEVERE] + registerDevice: [ deviceLibraryId='96577bb29e78f1240ecabc1f929da6f9', passTypeId: 'pass.com.qaroni.abanca.presco', serial: 'f534e630-64f3-4831-b7b4-63f77b4427ee' ]",
		"wallet-rest-api|SEVERE|16-02-22 19:05:17| [SEVERE] + registerDevice: [deviceLibraryId='96577bb29e78f1240ecabc1f929da6f9', passTypeId: 'pass.com.qaroni.abanca.presco', serial: 'eed59735-9093-4914-8939-20fd1eb26055' ]",
		"wallet-rest-api|INFO|16-02-22 19:05:18| [INFO] + whatChanget : [ passTypeId: 'pass.com.qaroni.abanca.xunta', deviceLibrary Id: 'a256dfbd0b2309b0570ab811d76b2274', updatedDate: 'null' ]",
		"wallet-rest-api|WARN|16-02-22 19:05:18| [WARN] + whatChanget : [ passTypeId: 'pass.com.qaroni.abanca.presco', deviceLibraryId: '96577bb29e78f1240ecabc1f929da6f9', updatedDate: 'null' ]"
);
// Simular la fase de Map
List<List<Map.Entry<String, Integer>>> mappedResults = documents.stream()
.map(MapReduceExample::mapFunction)
.collect(Collectors.toList());
// "Shuffle and Sort" (Flatten la lista de resultados de la fase de Map)
List<Map.Entry<String, Integer>> flattenedResults = mappedResults.stream()
.flatMap(List::stream)
.collect(Collectors.toList());
// Simular la fase de Reduce
Map<String, List<Integer>> reducedResults = flattenedResults.stream()
.collect(Collectors.groupingBy(Map.Entry::getKey,
Collectors.mapping(Map.Entry::getValue, Collectors.toList())));
// Imprimir resultados finales
List<Map.Entry<String, Integer>> finalResults = reducedResults.entrySet().stream()
.map(MapReduceExample::reduceFunction)
.collect(Collectors.toList());
System.out.println(finalResults);
}
}