//
// Takes fasta from LIMA and ISOSEQ REFINE inputs and splits their generated fastas
//

workflow CHUNKER {
    take:
    ch_refine_output     // Channel: [ meta[id, start_from ], fasta ]
    ch_input_mapping     // Channel: [ meta[id, start_from ], fasta, pbi ]
    chunk_mapping // value: integer (number of chunk to create)

    main:
    ch_refine_output
        .branch { meta, _fasta ->
            not_chunked : meta.start_from in [ "lima", "refine" ]
            chunked     : meta.start_from == "ccs"
        }
        .set { ch_seq_data }

    ch_seq_data
        .not_chunked
        .concat(ch_input_mapping.map { meta, fasta, _pbi -> [ meta, fasta ] })
        .splitFasta(
            by: chunk_mapping,
            decompress: true,
            file: "chunk",
            compress: true
        )
        .map { meta, file ->
            def chk = (file =~ /(chunk\.\d+)\.gz/)[ 0 ][ 1 ]
            def id_former = meta.id
            def id_new    = meta.id + "." + chk
            [ [ id:id_new, id_former:id_former ] , file ]
        }
        .set { ch_chunkies }

    ch_seq_data
        .chunked
        .concat(ch_chunkies)
        .set { fasta }


    emit:
    fasta
}
